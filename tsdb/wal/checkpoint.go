// Copyright 2018 The Prometheus Authors

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// CheckpointStats returns stats about a created checkpoint.
type CheckpointStats struct {
	DroppedSeries     int
	DroppedSamples    int
	DroppedTombstones int
	DroppedExemplars  int
	TotalSeries       int // Processed series including dropped ones.
	TotalSamples      int // Processed samples including dropped ones.
	TotalTombstones   int // Processed tombstones including dropped ones.
	TotalExemplars    int // Processed exemplars including dropped ones.
}

// LastCheckpoint returns the directory name and index of the most recent checkpoint.
// If dir does not contain any checkpoints, ErrNotFound is returned.
// WAL目录中有checkpoint目录
func LastCheckpoint(dir string) (string, int, error) {
	checkpoints, err := listCheckpoints(dir)
	if err != nil {
		return "", 0, err
	}

	if len(checkpoints) == 0 {
		return "", 0, record.ErrNotFound
	}

	checkpoint := checkpoints[len(checkpoints)-1]
	return filepath.Join(dir, checkpoint.name), checkpoint.index, nil
}

// DeleteCheckpoints deletes all checkpoints in a directory below a given index.
func DeleteCheckpoints(dir string, maxIndex int) error {
	checkpoints, err := listCheckpoints(dir)
	if err != nil {
		return err
	}

	errs := tsdb_errors.NewMulti()
	for _, checkpoint := range checkpoints {
		// 找到大于指定的checkpoints
		if checkpoint.index >= maxIndex {
			break
		}
		errs.Add(os.RemoveAll(filepath.Join(dir, checkpoint.name)))
	}
	return errs.Err()
}

// 默认的checkpoint的开头
const checkpointPrefix = "checkpoint."

// Checkpoint creates a compacted checkpoint of segments in range [from, to] in the given WAL.
// It includes the most recent checkpoint if it exists.
// All series not satisfying keep and samples/tombstones/exemplars below mint are dropped.
//
// The checkpoint is stored in a directory named checkpoint.N in the same
// segmented format as the original WAL itself.
// This makes it easy to read it through the WAL package and concatenate
// it with the original WAL.
func Checkpoint(logger log.Logger, w *WAL, from, to int, keep func(id chunks.HeadSeriesRef) bool, mint int64) (*CheckpointStats, error) {
	stats := &CheckpointStats{}
	var sgmReader io.ReadCloser

	// 创建segment
	level.Info(logger).Log("msg", "Creating checkpoint", "from_segment", from, "to_segment", to, "mint", mint)

	{
		var sgmRange []SegmentRange
		// 获取最新的chekpoint dir
		dir, idx, err := LastCheckpoint(w.Dir())
		// 获取上次的checkpoint 目录
		if err != nil && err != record.ErrNotFound {
			return nil, errors.Wrap(err, "find last checkpoint")
		}
		// 从下个index开始
		last := idx + 1
		if err == nil {
			// 不能超过上次merge的index
			if from > last {
				return nil, fmt.Errorf("unexpected gap to last checkpoint. expected:%v, requested:%v", last, from)
			}
			// Ignore WAL files below the checkpoint. They shouldn't exist to begin with.
			from = last

			// dir 目录
			sgmRange = append(sgmRange, SegmentRange{Dir: dir, Last: math.MaxInt32})
		}

		// 获取当前wal的文件列表
		sgmRange = append(sgmRange, SegmentRange{Dir: w.Dir(), First: from, Last: to})
		sgmReader, err = NewSegmentsRangeReader(sgmRange...)
		if err != nil {
			return nil, errors.Wrap(err, "create segment reader")
		}
		defer sgmReader.Close()
	}

	// 获取目录
	cpdir := checkpointDir(w.Dir(), to)
	cpdirtmp := cpdir + ".tmp"

	// 清除临时文件目录
	if err := os.RemoveAll(cpdirtmp); err != nil {
		return nil, errors.Wrap(err, "remove previous temporary checkpoint dir")
	}

	// 创建新的临时文件目录
	if err := os.MkdirAll(cpdirtmp, 0o777); err != nil {
		return nil, errors.Wrap(err, "create checkpoint dir")
	}

	// new 一个新的wal
	cp, err := New(nil, nil, cpdirtmp, w.CompressionEnabled())
	if err != nil {
		return nil, errors.Wrap(err, "open checkpoint")
	}

	// Ensures that an early return caused by an error doesn't leave any tmp files.
	defer func() {
		// 关闭当前wal
		cp.Close()
		// 移除临时文件
		os.RemoveAll(cpdirtmp)
	}()

	r := NewReader(sgmReader)

	var (
		series    []record.RefSeries
		samples   []record.RefSample
		tstones   []tombstones.Stone
		exemplars []record.RefExemplar
		dec       record.Decoder
		enc       record.Encoder
		buf       []byte
		recs      [][]byte
	)
	for r.Next() {
		series, samples, tstones, exemplars = series[:0], samples[:0], tstones[:0], exemplars[:0]

		// We don't reset the buffer since we batch up multiple records
		// before writing them to the checkpoint.
		// Remember where the record for this iteration starts.
		start := len(buf)
		// 获取当前的一条record
		rec := r.Record()

		// 获取最新的record相应的类型
		switch dec.Type(rec) {
		case record.Series:
			// 如果是series
			series, err = dec.Series(rec, series)
			if err != nil {
				return nil, errors.Wrap(err, "decode series")
			}
			// Drop irrelevant series in place.
			// 原地记录
			repl := series[:0]
			for _, s := range series {
				// 是否保留该series
				if keep(s.Ref) {
					repl = append(repl, s)
				}
			}
			if len(repl) > 0 {
				// 序列后的buf
				buf = enc.Series(repl, buf)
			}
			// 统计total series
			stats.TotalSeries += len(series)
			// 删除的series
			stats.DroppedSeries += len(series) - len(repl)

		case record.Samples:
			// 解析samples
			samples, err = dec.Samples(rec, samples)
			if err != nil {
				return nil, errors.Wrap(err, "decode samples")
			}
			// Drop irrelevant samples in place.
			repl := samples[:0]
			for _, s := range samples {
				if s.T >= mint {
					repl = append(repl, s)
				}
			}
			if len(repl) > 0 {
				buf = enc.Samples(repl, buf)
			}
			stats.TotalSamples += len(samples)
			stats.DroppedSamples += len(samples) - len(repl)

		case record.Tombstones:
			tstones, err = dec.Tombstones(rec, tstones)
			if err != nil {
				return nil, errors.Wrap(err, "decode deletes")
			}
			// Drop irrelevant tombstones in place.
			repl := tstones[:0]
			for _, s := range tstones {
				for _, iv := range s.Intervals {
					if iv.Maxt >= mint {
						repl = append(repl, s)
						break
					}
				}
			}
			if len(repl) > 0 {
				buf = enc.Tombstones(repl, buf)
			}
			stats.TotalTombstones += len(tstones)
			stats.DroppedTombstones += len(tstones) - len(repl)

		case record.Exemplars:
			exemplars, err = dec.Exemplars(rec, exemplars)
			if err != nil {
				return nil, errors.Wrap(err, "decode exemplars")
			}
			// Drop irrelevant exemplars in place.
			repl := exemplars[:0]
			for _, e := range exemplars {
				if e.T >= mint {
					repl = append(repl, e)
				}
			}
			if len(repl) > 0 {
				buf = enc.Exemplars(repl, buf)
			}
			stats.TotalExemplars += len(exemplars)
			stats.DroppedExemplars += len(exemplars) - len(repl)
		default:
			// Unknown record type, probably from a future Prometheus version.
			continue
		}
		if len(buf[start:]) == 0 {
			continue // All contents discarded.
		}
		recs = append(recs, buf[start:])

		// Flush records in 1 MB increments.
		// 超过1M了，flush一次
		if len(buf) > 1*1024*1024 {
			if err := cp.Log(recs...); err != nil {
				return nil, errors.Wrap(err, "flush records")
			}
			buf, recs = buf[:0], recs[:0]
		}
	}
	// If we hit any corruption during checkpointing, repairing is not an option.
	// The head won't know which series records are lost.
	if r.Err() != nil {
		return nil, errors.Wrap(r.Err(), "read segments")
	}

	// Flush remaining records.
	if err := cp.Log(recs...); err != nil {
		return nil, errors.Wrap(err, "flush records")
	}
	// 关闭临时文件
	if err := cp.Close(); err != nil {
		return nil, errors.Wrap(err, "close checkpoint")
	}

	// Sync temporary directory before rename.
	df, err := fileutil.OpenDir(cpdirtmp)
	if err != nil {
		return nil, errors.Wrap(err, "open temporary checkpoint directory")
	}
	if err := df.Sync(); err != nil {
		df.Close()
		return nil, errors.Wrap(err, "sync temporary checkpoint directory")
	}
	if err = df.Close(); err != nil {
		return nil, errors.Wrap(err, "close temporary checkpoint directory")
	}

	// 替换为正式的文件
	if err := fileutil.Replace(cpdirtmp, cpdir); err != nil {
		return nil, errors.Wrap(err, "rename checkpoint directory")
	}

	return stats, nil
}

func checkpointDir(dir string, i int) string {
	return filepath.Join(dir, fmt.Sprintf(checkpointPrefix+"%08d", i))
}

type checkpointRef struct {
	// 名字
	name string
	// 和index
	index int
}

// 获取当前的checkpoints
func listCheckpoints(dir string) (refs []checkpointRef, err error) {
	// 获取当前目录中的文件
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(files); i++ {
		fi := files[i]
		// 没有不是相关checkpoint的文件，直接过滤
		if !strings.HasPrefix(fi.Name(), checkpointPrefix) {
			continue
		}
		if !fi.IsDir() {
			return nil, errors.Errorf("checkpoint %s is not a directory", fi.Name())
		}
		// 获取inex
		idx, err := strconv.Atoi(fi.Name()[len(checkpointPrefix):])
		if err != nil {
			continue
		}

		refs = append(refs, checkpointRef{name: fi.Name(), index: idx})
	}

	sort.Slice(refs, func(i, j int) bool {
		// 进行排序
		return refs[i].index < refs[j].index
	})

	return refs, nil
}
