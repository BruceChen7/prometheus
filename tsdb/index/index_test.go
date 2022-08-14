// Copyright 2017 The Prometheus Authors
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

package index

import (
	"context"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type series struct {
	// 包含所有的label
	l labels.Labels
	// chunks信息
	chunks []chunks.Meta
}

type mockIndex struct {
	// series信息
	series map[storage.SeriesRef]series
	// 每个posting label包含的series
	// key是一个label name 和value
	postings map[labels.Label][]storage.SeriesRef
	// 符号信息
	symbols map[string]struct{}
}

func newMockIndex() mockIndex {
	// 用来模拟index
	ix := mockIndex{
		series:   make(map[storage.SeriesRef]series),
		postings: make(map[labels.Label][]storage.SeriesRef),
		symbols:  make(map[string]struct{}),
	}
	ix.postings[allPostingsKey] = []storage.SeriesRef{}
	return ix
}

// 返回所有的符号表
func (m mockIndex) Symbols() (map[string]struct{}, error) {
	return m.symbols, nil
}

// 增加series, ref是一个引用
func (m mockIndex) AddSeries(ref storage.SeriesRef, l labels.Labels, chunks ...chunks.Meta) error {
	if _, ok := m.series[ref]; ok {
		return errors.Errorf("series with reference %d already added", ref)
	}
	for _, lbl := range l {
		// 新增label name
		m.symbols[lbl.Name] = struct{}{}
		// 新增label value
		m.symbols[lbl.Value] = struct{}{}
		// 不存在该lable name对应，那么新增一个
		if _, ok := m.postings[lbl]; !ok {
			// posting中增加一种
			m.postings[lbl] = []storage.SeriesRef{}
		}
		// posting中新增一个series
		m.postings[lbl] = append(m.postings[lbl], ref)
	}
	// 所有的series集合中增加一个
	m.postings[allPostingsKey] = append(m.postings[allPostingsKey], ref)

	s := series{l: l}
	// Actual chunk data is not stored in the index.
	for _, c := range chunks {
		c.Chunk = nil
		// 新增chunk
		s.chunks = append(s.chunks, c)
	}
	// 新增一个series
	m.series[ref] = s

	return nil
}

func (m mockIndex) Close() error {
	return nil
}

// 所有label的可能取值
func (m mockIndex) LabelValues(name string) ([]string, error) {
	// 找到所有的可能取值
	values := []string{}
	for l := range m.postings {
		if l.Name == name {
			values = append(values, l.Value)
		}
	}
	return values, nil
}

// 根据label name 和value 来获取所有的posting
func (m mockIndex) Postings(name string, values ...string) (Postings, error) {
	// 获取的其实是seriesRef
	p := []Postings{}
	// 根据lable name 的不同取值来获取不同的postings
	for _, value := range values {
		l := labels.Label{Name: name, Value: value}
		p = append(p, m.SortedPostings(NewListPostings(m.postings[l])))
	}
	// 合并Postings
	return Merge(p...), nil
}

// 对posting进行排序
func (m mockIndex) SortedPostings(p Postings) Postings {
	// 遍历posting
	ep, err := ExpandPostings(p)
	if err != nil {
		return ErrPostings(errors.Wrap(err, "expand postings"))
	}

	// 排序
	sort.Slice(ep, func(i, j int) bool {
		// 比较label的name
		return labels.Compare(m.series[ep[i]].l, m.series[ep[j]].l) < 0
	})
	// 返回postings
	return NewListPostings(ep)
}

func (m mockIndex) Series(ref storage.SeriesRef, lset *labels.Labels, chks *[]chunks.Meta) error {
	s, ok := m.series[ref]
	if !ok {
		return errors.New("not found")
	}
	*lset = append((*lset)[:0], s.l...)
	*chks = append((*chks)[:0], s.chunks...)

	return nil
}

// 测试文件的创建
func TestIndexRW_Create_Open(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_index_create")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()

	fn := filepath.Join(dir, indexFilename)

	// An empty index must still result in a readable file.
	iw, err := NewWriter(context.Background(), fn)
	require.NoError(t, err)
	require.NoError(t, iw.Close())

	ir, err := NewFileReader(fn)
	require.NoError(t, err)
	require.NoError(t, ir.Close())

	// Modify magic header must cause open to fail.
	f, err := os.OpenFile(fn, os.O_WRONLY, 0o666)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0, 0}, 0)
	require.NoError(t, err)
	f.Close()

	_, err = NewFileReader(dir)
	require.Error(t, err)
}

// 读写postings
func TestIndexRW_Postings(t *testing.T) {
	// 临时目录
	dir, err := ioutil.TempDir("", "test_index_postings")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()

	// 文件的全路径
	fn := filepath.Join(dir, indexFilename)

	// 新增一个writer
	iw, err := NewWriter(context.Background(), fn)
	require.NoError(t, err)

	// 每个series的lable
	series := []labels.Labels{
		// 增加了lable name 和value
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "1", "b", "3"),
		labels.FromStrings("a", "1", "b", "4"),
	}

	// 添加符号
	require.NoError(t, iw.AddSymbol("1"))
	require.NoError(t, iw.AddSymbol("2"))
	require.NoError(t, iw.AddSymbol("3"))
	require.NoError(t, iw.AddSymbol("4"))
	require.NoError(t, iw.AddSymbol("a"))
	require.NoError(t, iw.AddSymbol("b"))

	// Postings lists are only written if a series with the respective
	// reference was added before.
	// 增加series
	// 第一个参数是seriesRef
	require.NoError(t, iw.AddSeries(1, series[0]))
	// 这里用来序列化
	require.NoError(t, iw.AddSeries(2, series[1]))
	require.NoError(t, iw.AddSeries(3, series[2]))
	require.NoError(t, iw.AddSeries(4, series[3]))

	require.NoError(t, iw.Close())

	// 用来读
	ir, err := NewFileReader(fn)
	require.NoError(t, err)

	// 获取label为a = 1 的postings
	p, err := ir.Postings("a", "1")
	require.NoError(t, err)

	var l labels.Labels
	var c []chunks.Meta

	for i := 0; p.Next(); i++ {
		// 获取chunk meta， 和label
		err := ir.Series(p.At(), &l, &c)

		require.NoError(t, err)
		// 获取chunk meta，没有
		require.Equal(t, 0, len(c))
		// 对应的lable的相等
		require.Equal(t, series[i], l)
	}
	// 没有错误
	require.NoError(t, p.Err())

	// The label indices are no longer used, so test them by hand here.
	labelIndices := map[string][]string{}
	// offset table
	require.NoError(t, ReadOffsetTable(ir.b, ir.toc.LabelIndicesTable, func(key []string, off uint64, _ int) error {
		// 获取key的长度
		if len(key) != 1 {
			return errors.Errorf("unexpected key length for label indices table %d", len(key))
		}

		d := encoding.NewDecbufAt(ir.b, int(off), castagnoliTable)
		vals := []string{}
		nc := d.Be32int()
		if nc != 1 {
			return errors.Errorf("unexpected number of label indices table names %d", nc)
		}
		for i := d.Be32(); i > 0; i-- {
			// 获取符号
			// 这是个引用
			v, err := ir.lookupSymbol(d.Be32())
			if err != nil {
				return err
			}
			vals = append(vals, v)
		}
		// 获取label a 对应的val
		labelIndices[key[0]] = vals
		return d.Err()
	}))
	require.Equal(t, map[string][]string{
		// a的可能取值
		"a": {"1"},
		// b的可能取值
		"b": {"1", "2", "3", "4"},
	}, labelIndices)

	require.NoError(t, ir.Close())
}

func TestPostingsMany(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_postings_many")
	require.NoError(t, err)
	defer func() {
		// 用来删除文件
		require.NoError(t, os.RemoveAll(dir))
	}()

	fn := filepath.Join(dir, indexFilename)

	iw, err := NewWriter(context.Background(), fn)
	require.NoError(t, err)

	// Create a label in the index which has 999 values.
	symbols := map[string]struct{}{}
	series := []labels.Labels{}
	// 1000个series
	for i := 1; i < 1000; i++ {
		v := fmt.Sprintf("%03d", i)
		// 每个lable name，对应的value
		series = append(series, labels.FromStrings("i", v, "foo", "bar"))
		symbols[v] = struct{}{}
	}
	// 加入label
	symbols["i"] = struct{}{}
	symbols["foo"] = struct{}{}
	symbols["bar"] = struct{}{}
	// symbs列表
	syms := []string{}
	for s := range symbols {
		syms = append(syms, s)
	}
	// 用来写入symbol
	// 按照字典序来排序
	sort.Strings(syms)
	for _, s := range syms {
		require.NoError(t, iw.AddSymbol(s))
	}

	// 添加series
	for i, s := range series {
		require.NoError(t, iw.AddSeries(storage.SeriesRef(i), s))
	}
	require.NoError(t, iw.Close())

	// 反序列化后来读取
	ir, err := NewFileReader(fn)
	require.NoError(t, err)
	defer func() { require.NoError(t, ir.Close()) }()

	// 不同的cases
	cases := []struct {
		in []string
	}{
		// Simple cases, everything is present.
		{in: []string{"002"}},
		{in: []string{"031", "032", "033"}},
		{in: []string{"032", "033"}},
		{in: []string{"127", "128"}},
		{in: []string{"127", "128", "129"}},
		{in: []string{"127", "129"}},
		{in: []string{"128", "129"}},
		{in: []string{"998", "999"}},
		{in: []string{"999"}},
		// Before actual values.
		{in: []string{"000"}},
		{in: []string{"000", "001"}},
		{in: []string{"000", "002"}},
		// After actual values.
		{in: []string{"999a"}},
		{in: []string{"999", "999a"}},
		{in: []string{"998", "999", "999a"}},
		// In the middle of actual values.
		{in: []string{"126a", "127", "128"}},
		{in: []string{"127", "127a", "128"}},
		{in: []string{"127", "127a", "128", "128a", "129"}},
		{in: []string{"127", "128a", "129"}},
		{in: []string{"128", "128a", "129"}},
		{in: []string{"128", "129", "129a"}},
		{in: []string{"126a", "126b", "127", "127a", "127b", "128", "128a", "128b", "129", "129a", "129b"}},
	}

	for _, c := range cases {
		// 取lable i = 对应的value
		it, err := ir.Postings("i", c.in...)
		require.NoError(t, err)

		got := []string{}
		var lbls labels.Labels
		var metas []chunks.Meta
		for it.Next() {
			// 获取label 没有问题
			require.NoError(t, ir.Series(it.At(), &lbls, &metas))
			// 获取了label的可能取值
			got = append(got, lbls.Get("i"))
		}
		require.NoError(t, it.Err())
		exp := []string{}
		for _, e := range c.in {
			// 找到相关的label
			if _, ok := symbols[e]; ok && e != "l" {
				exp = append(exp, e)
			}
		}
		require.Equal(t, exp, got, fmt.Sprintf("input: %v", c.in))
	}
}

// 端对端测试
func TestPersistence_index_e2e(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_persistence_e2e")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()

	lbls, err := labels.ReadLabels(filepath.Join("..", "testdata", "20kseries.json"), 20000)
	require.NoError(t, err)

	// Sort labels as the index writer expects series in sorted order.
	sort.Sort(labels.Slice(lbls))

	symbols := map[string]struct{}{}
	for _, lset := range lbls {
		for _, l := range lset {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		}
	}

	var input indexWriterSeriesSlice

	// Generate ChunkMetas for every label set.
	for i, lset := range lbls {
		var metas []chunks.Meta

		for j := 0; j <= (i % 20); j++ {
			metas = append(metas, chunks.Meta{
				MinTime: int64(j * 10000),
				MaxTime: int64((j + 1) * 10000),
				Ref:     chunks.ChunkRef(rand.Uint64()),
				Chunk:   chunkenc.NewXORChunk(),
			})
		}
		input = append(input, &indexWriterSeries{
			labels: lset,
			chunks: metas,
		})
	}

	iw, err := NewWriter(context.Background(), filepath.Join(dir, indexFilename))
	require.NoError(t, err)

	syms := []string{}
	for s := range symbols {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	for _, s := range syms {
		require.NoError(t, iw.AddSymbol(s))
	}

	// Population procedure as done by compaction.
	var (
		postings = NewMemPostings()
		values   = map[string]map[string]struct{}{}
	)

	mi := newMockIndex()

	for i, s := range input {
		err = iw.AddSeries(storage.SeriesRef(i), s.labels, s.chunks...)
		require.NoError(t, err)
		require.NoError(t, mi.AddSeries(storage.SeriesRef(i), s.labels, s.chunks...))

		for _, l := range s.labels {
			valset, ok := values[l.Name]
			if !ok {
				valset = map[string]struct{}{}
				values[l.Name] = valset
			}
			valset[l.Value] = struct{}{}
		}
		postings.Add(storage.SeriesRef(i), s.labels)
	}

	err = iw.Close()
	require.NoError(t, err)

	ir, err := NewFileReader(filepath.Join(dir, indexFilename))
	require.NoError(t, err)

	for p := range mi.postings {
		gotp, err := ir.Postings(p.Name, p.Value)
		require.NoError(t, err)

		expp, err := mi.Postings(p.Name, p.Value)
		require.NoError(t, err)

		var lset, explset labels.Labels
		var chks, expchks []chunks.Meta

		for gotp.Next() {
			require.True(t, expp.Next())

			ref := gotp.At()

			err := ir.Series(ref, &lset, &chks)
			require.NoError(t, err)

			err = mi.Series(expp.At(), &explset, &expchks)
			require.NoError(t, err)
			require.Equal(t, explset, lset)
			require.Equal(t, expchks, chks)
		}
		require.False(t, expp.Next(), "Expected no more postings for %q=%q", p.Name, p.Value)
		require.NoError(t, gotp.Err())
	}

	labelPairs := map[string][]string{}
	for l := range mi.postings {
		labelPairs[l.Name] = append(labelPairs[l.Name], l.Value)
	}
	for k, v := range labelPairs {
		sort.Strings(v)

		res, err := ir.SortedLabelValues(k)
		require.NoError(t, err)

		require.Equal(t, len(v), len(res))
		for i := 0; i < len(v); i++ {
			require.Equal(t, v[i], res[i])
		}
	}

	gotSymbols := []string{}
	it := ir.Symbols()
	for it.Next() {
		gotSymbols = append(gotSymbols, it.At())
	}
	require.NoError(t, it.Err())
	expSymbols := []string{}
	for s := range mi.symbols {
		expSymbols = append(expSymbols, s)
	}
	sort.Strings(expSymbols)
	require.Equal(t, expSymbols, gotSymbols)

	require.NoError(t, ir.Close())
}

func TestDecbufUvarintWithInvalidBuffer(t *testing.T) {
	b := realByteSlice([]byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81})

	db := encoding.NewDecbufUvarintAt(b, 0, castagnoliTable)
	require.Error(t, db.Err())
}

func TestReaderWithInvalidBuffer(t *testing.T) {
	b := realByteSlice([]byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81})

	_, err := NewReader(b)
	require.Error(t, err)
}

// TestNewFileReaderErrorNoOpenFiles ensures that in case of an error no file remains open.
func TestNewFileReaderErrorNoOpenFiles(t *testing.T) {
	dir := testutil.NewTemporaryDirectory("block", t)

	idxName := filepath.Join(dir.Path(), "index")
	err := ioutil.WriteFile(idxName, []byte("corrupted contents"), 0o666)
	require.NoError(t, err)

	_, err = NewFileReader(idxName)
	require.Error(t, err)

	// dir.Close will fail on Win if idxName fd is not closed on error path.
	dir.Close()
}

func TestSymbols(t *testing.T) {
	buf := encoding.Encbuf{}

	// Add prefix to the buffer to simulate symbols as part of larger buffer.
	buf.PutUvarintStr("something")

	symbolsStart := buf.Len()
	buf.PutBE32int(204) // Length of symbols table.
	buf.PutBE32int(100) // Number of symbols.
	for i := 0; i < 100; i++ {
		// i represents index in unicode characters table.
		buf.PutUvarintStr(string(rune(i))) // Symbol.
	}
	checksum := crc32.Checksum(buf.Get()[symbolsStart+4:], castagnoliTable)
	buf.PutBE32(checksum) // Check sum at the end.

	s, err := NewSymbols(realByteSlice(buf.Get()), FormatV2, symbolsStart)
	require.NoError(t, err)

	// We store only 4 offsets to symbols.
	require.Equal(t, 32, s.Size())

	for i := 99; i >= 0; i-- {
		s, err := s.Lookup(uint32(i))
		require.NoError(t, err)
		require.Equal(t, string(rune(i)), s)
	}
	_, err = s.Lookup(100)
	require.Error(t, err)

	for i := 99; i >= 0; i-- {
		r, err := s.ReverseLookup(string(rune(i)))
		require.NoError(t, err)
		require.Equal(t, uint32(i), r)
	}
	_, err = s.ReverseLookup(string(rune(100)))
	require.Error(t, err)

	iter := s.Iter()
	i := 0
	for iter.Next() {
		require.Equal(t, string(rune(i)), iter.At())
		i++
	}
	require.NoError(t, iter.Err())
}
