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

// The code in this file was largely written by Damian Gryski as part of
// https://github.com/dgryski/go-tsz and published under the license below.
// It was modified to accommodate reading from byte slices without modifying
// the underlying bytes, which would panic when reading from mmap'd
// read-only byte slices.

// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package chunkenc

import (
	"encoding/binary"
	"math"
	"math/bits"
)

const (
	chunkCompactCapacityThreshold = 32
)

// XORChunk holds XOR encoded sample data.
type XORChunk struct {
	b bstream
}

// NewXORChunk returns a new chunk with XOR encoding of the given size.
func NewXORChunk() *XORChunk {
	// 预留出2byte，放number of sample
	b := make([]byte, 2, 128)
	return &XORChunk{b: bstream{stream: b, count: 0}}
}

// Encoding returns the encoding type.
func (c *XORChunk) Encoding() Encoding {
	// 编码方式
	return EncXOR
}

// Bytes returns the underlying byte slice of the chunk.
func (c *XORChunk) Bytes() []byte {
	// 获取底层的byte stream
	return c.b.bytes()
}

// NumSamples returns the number of samples in the chunk.
func (c *XORChunk) NumSamples() int {
	// 获取前2个字节
	return int(binary.BigEndian.Uint16(c.Bytes()))
}

// 缩容
func (c *XORChunk) Compact() {
	if l := len(c.b.stream); cap(c.b.stream) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b.stream)
		c.b.stream = buf
	}
}

// Appender implements the Chunk interface.
// 添加一个采样
func (c *XORChunk) Appender() (Appender, error) {
	it := c.iterator(nil)

	// To get an appender we must know the state it would have if we had
	// appended all existing data from scratch.
	// We iterate through the end and populate via the iterator's state.
	// 迭代到最后
	for it.Next() {
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	// 返回一个迭代器
	a := &xorAppender{
		b:        &c.b,
		t:        it.t,
		v:        it.val,
		tDelta:   it.tDelta,
		leading:  it.leading,
		trailing: it.trailing,
	}
	if binary.BigEndian.Uint16(a.b.bytes()) == 0 {
		a.leading = 0xff
	}
	return a, nil
}

func (c *XORChunk) iterator(it Iterator) *xorIterator {
	// Should iterators guarantee to act on a copy of the data so it doesn't lock append?
	// When using striped locks to guard access to chunks, probably yes.
	// Could only copy data if the chunk is not completed yet.
	if xorIter, ok := it.(*xorIterator); ok {
		xorIter.Reset(c.b.bytes())
		return xorIter
	}
	// 新建一个reader
	return &xorIterator{
		br:       newBReader(c.b.bytes()[2:]),
		numTotal: binary.BigEndian.Uint16(c.b.bytes()),
		numRead:  0,
		t:        math.MinInt64,
		val:      0,
		leading:  0,
		trailing: 0,
		tDelta:   0,
		err:      nil,
	}
}

// Iterator implements the Chunk interface.
func (c *XORChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

type xorAppender struct {
	b *bstream

	// 关联的时间戳
	t      int64
	v      float64
	tDelta uint64

	// 开始一个字节为0xff
	leading  uint8
	trailing uint8
}

// 添加一个时间戳和value
func (a *xorAppender) Append(t int64, v float64) {
	var tDelta uint64
	num := binary.BigEndian.Uint16(a.b.bytes())

	if num == 0 {
		buf := make([]byte, binary.MaxVarintLen64)
		// binary.PutVarint返回编码后的字节数
		for _, b := range buf[:binary.PutVarint(buf, t)] {
			// 先写时间戳
			a.b.writeByte(b)
		}
		// 然后些值
		a.b.writeBits(math.Float64bits(v), 64)

	} else if num == 1 { // 如果是前面写了一个
		// 第一个delta
		tDelta = uint64(t - a.t)

		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutUvarint(buf, tDelta)] {
			// 写tDelta
			a.b.writeByte(b)
		}

		// 写v float delta
		a.writeVDelta(v)

	} else {
		// 比较delta of delta
		tDelta = uint64(t - a.t)
		dod := int64(tDelta - a.tDelta)

		// Gorilla has a max resolution of seconds, Prometheus milliseconds.
		// Thus we use higher value range steps with larger bit size.
		switch {
		case dod == 0:
			a.b.writeBit(zero)
		case bitRange(dod, 14): // 14个bit能表示
			a.b.writeBits(0b10, 2) // 写入前缀2
			a.b.writeBits(uint64(dod), 14)
		case bitRange(dod, 17): // 17个bit能表示
			a.b.writeBits(0b110, 3) // 写入前缀3
			a.b.writeBits(uint64(dod), 17)
		case bitRange(dod, 20): // 20 bit能表示
			a.b.writeBits(0b1110, 4)
			a.b.writeBits(uint64(dod), 20) // 写入dod，用20个字节来表示
		default:
			a.b.writeBits(0b1111, 4) // 前缀
			a.b.writeBits(uint64(dod), 64)
		}

		a.writeVDelta(v)
	}
	// 更新当前的t
	a.t = t
	// 更新当前的值
	a.v = v
	// 添加当前stream中的number
	binary.BigEndian.PutUint16(a.b.bytes(), num+1)
	a.tDelta = tDelta
}

// bitRange returns whether the given integer can be represented by nbits.
// See docs/bstream.md.
// 能否用相应的nbits来表示
// 如果nbits位3，那么能够表示的范围是-3到4
func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}

func (a *xorAppender) writeVDelta(v float64) {
	// 进行异或
	vDelta := math.Float64bits(v) ^ math.Float64bits(a.v)
	// 如果相同
	if vDelta == 0 {
		a.b.writeBit(zero)
		return
	}
	// 添加1 bit
	a.b.writeBit(one)

	// 最高位有多少个0
	leading := uint8(bits.LeadingZeros64(vDelta))
	// 结尾位有多少个0
	trailing := uint8(bits.TrailingZeros64(vDelta))

	// Clamp number of leading zeros to avoid overflow when encoding.
	if leading >= 32 {
		// 最多位31位
		leading = 31
	}

	// 上次的a.leading不为11111
	// 意思不是第一次
	// 足够5bit来保存
	if a.leading != 0xff && leading >= a.leading && trailing >= a.trailing {
		// 再次写入一个0
		a.b.writeBit(zero)
		// 用上次的leading 和traing来记录
		a.b.writeBits(vDelta>>a.trailing, 64-int(a.leading)-int(a.trailing))
	} else { // leading 相同的太多
		// a.leading == 0xff ||
		// leading < a.leading 相同的leading 没有上次leading的多 ||
		// trailing < a.trailing 小于上次相同的traing

		// 更新leading 和 trailing
		a.leading, a.trailing = leading, trailing

		// 再写一个1
		a.b.writeBit(one)
		// 用5位来表示相同的leading 位数
		a.b.writeBits(uint64(leading), 5)

		// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
		// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
		// So instead we write out a 0 and adjust it back to 64 on unpacking.
		// 不同的位数
		sigbits := 64 - leading - trailing
		// 不同的位数
		a.b.writeBits(uint64(sigbits), 6)
		// 写下值
		a.b.writeBits(vDelta>>trailing, int(sigbits))
	}
}

// 抽象出来的迭代器
type xorIterator struct {
	br       bstreamReader
	numTotal uint16
	numRead  uint16

	t   int64
	val float64

	leading  uint8
	trailing uint8

	tDelta uint64
	err    error
}

func (it *xorIterator) Seek(t int64) bool {
	if it.err != nil {
		return false
	}

	for t > it.t || it.numRead == 0 {
		if !it.Next() {
			return false
		}
	}
	return true
}

func (it *xorIterator) At() (int64, float64) {
	return it.t, it.val
}

func (it *xorIterator) Err() error {
	return it.err
}

func (it *xorIterator) Reset(b []byte) {
	// The first 2 bytes contain chunk headers.
	// We skip that for actual samples.
	// 除去header
	it.br = newBReader(b[2:])
	it.numTotal = binary.BigEndian.Uint16(b)

	it.numRead = 0
	it.t = 0
	it.val = 0
	it.leading = 0
	it.trailing = 0
	it.tDelta = 0
	it.err = nil
}

func (it *xorIterator) Next() bool {
	if it.err != nil || it.numRead == it.numTotal {
		return false
	}

	// 还没有读过
	if it.numRead == 0 {
		// 从bstream中读取
		t, err := binary.ReadVarint(&it.br)
		if err != nil {
			it.err = err
			return false
		}
		v, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		// 获取时间戳
		it.t = t
		// 获取值
		it.val = math.Float64frombits(v)

		// 读取的数 + 1
		it.numRead++
		return true
	}
	if it.numRead == 1 {
		tDelta, err := binary.ReadUvarint(&it.br)
		if err != nil {
			it.err = err
			return false
		}
		// 设置delta
		it.tDelta = tDelta
		// 设置时间戳
		it.t = it.t + int64(it.tDelta)

		return it.readValue()
	}

	var d byte
	// read delta-of-delta
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.br.readBitFast()
		if err != nil {
			bit, err = it.br.readBit()
		}
		if err != nil {
			it.err = err
			return false
		}
		// 读取到0为为止
		if bit == zero {
			break
		}
		d |= 1
	}
	var sz uint8
	var dod int64
	switch d {
	case 0b0:
		// dod == 0
	case 0b10:
		// dod使用的bit数
		sz = 14
	case 0b110:
		// dod使用的bit数
		sz = 17
	case 0b1110:
		sz = 20
	case 0b1111:
		// Do not use fast because it's very unlikely it will succeed.
		bits, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}

		dod = int64(bits)
	}

	if sz != 0 {
		// 读取对应bit的数
		bits, err := it.br.readBitsFast(sz)
		if err != nil {
			bits, err = it.br.readBits(sz)
		}
		if err != nil {
			it.err = err
			return false
		}

		// Account for negative numbers, which come back as high unsigned numbers.
		// See docs/bstream.md.
		if bits > (1 << (sz - 1)) {
			bits -= 1 << sz
		}
		// 计算真正的dod的数值
		dod = int64(bits)
	}

	it.tDelta = uint64(int64(it.tDelta) + dod)
	it.t = it.t + int64(it.tDelta)

	// 获取value值
	return it.readValue()
}

func (it *xorIterator) readValue() bool {
	// 读取一个bit
	bit, err := it.br.readBitFast()
	if err != nil {
		bit, err = it.br.readBit()
	}
	if err != nil {
		it.err = err
		return false
	}

	if bit == zero {
		// it.val = it.val
	} else {
		bit, err := it.br.readBitFast()
		if err != nil {
			bit, err = it.br.readBit()
		}
		if err != nil {
			it.err = err
			return false
		}
		if bit == zero {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := it.br.readBitsFast(5)
			if err != nil {
				bits, err = it.br.readBits(5)
			}
			if err != nil {
				it.err = err
				return false
			}
			// 读取当前leading中相同的位数
			it.leading = uint8(bits)

			bits, err = it.br.readBitsFast(6)
			if err != nil {
				bits, err = it.br.readBits(6)
			}
			if err != nil {
				it.err = err
				return false
			}
			// 读取不同的部分
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			// 获取traling中相同的位数
			it.trailing = 64 - it.leading - mbits
		}

		mbits := 64 - it.leading - it.trailing
		bits, err := it.br.readBitsFast(mbits)
		if err != nil {
			bits, err = it.br.readBits(mbits)
		}
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.val)
		vbits ^= bits << it.trailing
		// 恢复value
		it.val = math.Float64frombits(vbits)
	}

	// 读取数 + 1
	it.numRead++
	return true
}
