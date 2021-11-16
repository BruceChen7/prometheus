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

package record

import (
	"math"
	"sort"

	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// Type represents the data type of a record.
type Type uint8

const (
	// Unknown is returned for unrecognised WAL record types.
	Unknown Type = 255
	// Series is used to match WAL records of type Series.
	Series Type = 1
	// Samples is used to match WAL records of type Samples.
	Samples Type = 2
	// Tombstones is used to match WAL records of type Tombstones.
	Tombstones Type = 3
	// Exemplars is used to match WAL records of type Exemplars.
	Exemplars Type = 4
)

// ErrNotFound is returned if a looked up resource was not found. Duplicate ErrNotFound from head.go.
var ErrNotFound = errors.New("not found")

// RefSeries is the series labels with the series ID.
type RefSeries struct {
	Ref chunks.HeadSeriesRef
	// 多个label
	Labels labels.Labels
}

// RefSample is a timestamp/value pair associated with a reference to a series.
type RefSample struct {
	Ref chunks.HeadSeriesRef
	// 时间
	T int64
	// 值
	V float64
}

// RefExemplar is an exemplar with it's labels, timestamp, value the exemplar was collected/observed with, and a reference to a series.
type RefExemplar struct {
	Ref    chunks.HeadSeriesRef
	T      int64
	V      float64
	Labels labels.Labels
}

// Decoder decodes series, sample, and tombstone records.
// The zero value is ready to use.
type Decoder struct{}

// Type returns the type of the record.
// Returns RecordUnknown if no valid record type is found.
func (d *Decoder) Type(rec []byte) Type {
	if len(rec) < 1 {
		return Unknown
	}
	switch t := Type(rec[0]); t {
	case Series, Samples, Tombstones, Exemplars:
		return t
	}
	return Unknown
}

// Series appends series in rec to the given slice.
func (d *Decoder) Series(rec []byte, series []RefSeries) ([]RefSeries, error) {
	dec := encoding.Decbuf{B: rec}

	// record类型不是series
	if Type(dec.Byte()) != Series {
		return nil, errors.New("invalid record type")
	}

	// ┌────────────────────────────────────────────┐
	// │ type = 1 <1b>                              │
	// ├────────────────────────────────────────────┤
	// │ ┌─────────┬──────────────────────────────┐ │
	// │ │ id <8b> │ n = len(labels) <uvarint>    │ │
	// │ ├─────────┴────────────┬─────────────────┤ │
	// │ │ len(str_1) <uvarint> │ str_1 <bytes>   │ │
	// │ ├──────────────────────┴─────────────────┤ │
	// │ │  ...                                   │ │
	// │ ├───────────────────────┬────────────────┤ │
	// │ │ len(str_2n) <uvarint> │ str_2n <bytes> │ │
	// │ └───────────────────────┴────────────────┘ │
	// │                  . . .                     │
	// └────────────────────────────────────────────┘

	for len(dec.B) > 0 && dec.Err() == nil {
		// 获取该series 的id
		ref := storage.SeriesRef(dec.Be64())

		// 几个label
		lset := make(labels.Labels, dec.Uvarint())

		for i := range lset {
			lset[i].Name = dec.UvarintStr()
			lset[i].Value = dec.UvarintStr()
		}
		sort.Sort(lset)

		series = append(series, RefSeries{
			// 生成一个headchuank ref
			// HeadChunkRef packs a HeadSeriesRef and a ChunkID into a global 8 Byte ID.
			Ref:    chunks.HeadSeriesRef(ref),
			Labels: lset,
		})
	}
	if dec.Err() != nil {
		return nil, dec.Err()
	}
	// 还剩余数据
	if len(dec.B) > 0 {
		return nil, errors.Errorf("unexpected %d bytes left in entry", len(dec.B))
	}
	return series, nil
}

// Samples appends samples in rec to the given slice.
// 解析
func (d *Decoder) Samples(rec []byte, samples []RefSample) ([]RefSample, error) {
	dec := encoding.Decbuf{B: rec}

	// 类型不对
	if Type(dec.Byte()) != Samples {
		return nil, errors.New("invalid record type")
	}
	// sample的个数
	if dec.Len() == 0 {
		return samples, nil
	}
	var (
		// base id
		baseRef = dec.Be64()
		// base 时间
		baseTime = dec.Be64int64()
	)
	// 一直到byte没有
	for len(dec.B) > 0 && dec.Err() == nil {
		dref := dec.Varint64()
		dtime := dec.Varint64()
		val := dec.Be64()

		samples = append(samples, RefSample{
			// series id号
			Ref: chunks.HeadSeriesRef(int64(baseRef) + dref),
			// 时间和值
			T: baseTime + dtime,
			V: math.Float64frombits(val),
		})
	}

	if dec.Err() != nil {
		return nil, errors.Wrapf(dec.Err(), "decode error after %d samples", len(samples))
	}
	if len(dec.B) > 0 {
		return nil, errors.Errorf("unexpected %d bytes left in entry", len(dec.B))
	}
	return samples, nil
}

// Tombstones appends tombstones in rec to the given slice.
func (d *Decoder) Tombstones(rec []byte, tstones []tombstones.Stone) ([]tombstones.Stone, error) {
	dec := encoding.Decbuf{B: rec}

	if Type(dec.Byte()) != Tombstones {
		return nil, errors.New("invalid record type")
	}
	for dec.Len() > 0 && dec.Err() == nil {
		tstones = append(tstones, tombstones.Stone{
			Ref: storage.SeriesRef(dec.Be64()),
			Intervals: tombstones.Intervals{
				{Mint: dec.Varint64(), Maxt: dec.Varint64()},
			},
		})
	}
	if dec.Err() != nil {
		return nil, dec.Err()
	}
	if len(dec.B) > 0 {
		return nil, errors.Errorf("unexpected %d bytes left in entry", len(dec.B))
	}
	return tstones, nil
}

func (d *Decoder) Exemplars(rec []byte, exemplars []RefExemplar) ([]RefExemplar, error) {
	dec := encoding.Decbuf{B: rec}
	t := Type(dec.Byte())
	if t != Exemplars {
		return nil, errors.New("invalid record type")
	}

	return d.ExemplarsFromBuffer(&dec, exemplars)
}

func (d *Decoder) ExemplarsFromBuffer(dec *encoding.Decbuf, exemplars []RefExemplar) ([]RefExemplar, error) {
	if dec.Len() == 0 {
		return exemplars, nil
	}
	var (
		baseRef  = dec.Be64()
		baseTime = dec.Be64int64()
	)
	for len(dec.B) > 0 && dec.Err() == nil {
		dref := dec.Varint64()
		dtime := dec.Varint64()
		val := dec.Be64()

		lset := make(labels.Labels, dec.Uvarint())
		for i := range lset {
			lset[i].Name = dec.UvarintStr()
			lset[i].Value = dec.UvarintStr()
		}
		sort.Sort(lset)

		exemplars = append(exemplars, RefExemplar{
			Ref:    chunks.HeadSeriesRef(baseRef + uint64(dref)),
			T:      baseTime + dtime,
			V:      math.Float64frombits(val),
			Labels: lset,
		})
	}

	if dec.Err() != nil {
		return nil, errors.Wrapf(dec.Err(), "decode error after %d exemplars", len(exemplars))
	}
	if len(dec.B) > 0 {
		return nil, errors.Errorf("unexpected %d bytes left in entry", len(dec.B))
	}
	return exemplars, nil
}

// Encoder encodes series, sample, and tombstones records.
// The zero value is ready to use.
type Encoder struct{}

// Series appends the encoded series to b and returns the resulting slice.
func (e *Encoder) Series(series []RefSeries, b []byte) []byte {
	// buffer
	buf := encoding.Encbuf{B: b}
	// 设置type
	buf.PutByte(byte(Series))

	for _, s := range series {
		// 设置series id
		buf.PutBE64(uint64(s.Ref))
		buf.PutUvarint(len(s.Labels))

		for _, l := range s.Labels {
			buf.PutUvarintStr(l.Name)
			buf.PutUvarintStr(l.Value)
		}
	}
	// 获取序列化后的byte
	return buf.Get()
}

// Samples appends the encoded samples to b and returns the resulting slice.
func (e *Encoder) Samples(samples []RefSample, b []byte) []byte {
	buf := encoding.Encbuf{B: b}
	buf.PutByte(byte(Samples))

	if len(samples) == 0 {
		return buf.Get()
	}

	// Store base timestamp and base reference number of first sample.
	// All samples encode their timestamp and ref as delta to those.
	first := samples[0]

	buf.PutBE64(uint64(first.Ref))
	buf.PutBE64int64(first.T)

	for _, s := range samples {
		buf.PutVarint64(int64(s.Ref) - int64(first.Ref))
		buf.PutVarint64(s.T - first.T)
		buf.PutBE64(math.Float64bits(s.V))
	}
	return buf.Get()
}

// Tombstones appends the encoded tombstones to b and returns the resulting slice.
func (e *Encoder) Tombstones(tstones []tombstones.Stone, b []byte) []byte {
	buf := encoding.Encbuf{B: b}
	buf.PutByte(byte(Tombstones))

	for _, s := range tstones {
		for _, iv := range s.Intervals {
			buf.PutBE64(uint64(s.Ref))
			buf.PutVarint64(iv.Mint)
			buf.PutVarint64(iv.Maxt)
		}
	}
	return buf.Get()
}

func (e *Encoder) Exemplars(exemplars []RefExemplar, b []byte) []byte {
	buf := encoding.Encbuf{B: b}
	buf.PutByte(byte(Exemplars))

	if len(exemplars) == 0 {
		return buf.Get()
	}

	e.EncodeExemplarsIntoBuffer(exemplars, &buf)

	return buf.Get()
}

func (e *Encoder) EncodeExemplarsIntoBuffer(exemplars []RefExemplar, buf *encoding.Encbuf) {
	// Store base timestamp and base reference number of first sample.
	// All samples encode their timestamp and ref as delta to those.
	first := exemplars[0]

	buf.PutBE64(uint64(first.Ref))
	buf.PutBE64int64(first.T)

	for _, ex := range exemplars {
		buf.PutVarint64(int64(ex.Ref) - int64(first.Ref))
		buf.PutVarint64(ex.T - first.T)
		buf.PutBE64(math.Float64bits(ex.V))

		buf.PutUvarint(len(ex.Labels))
		for _, l := range ex.Labels {
			buf.PutUvarintStr(l.Name)
			buf.PutUvarintStr(l.Value)
		}
	}
}
