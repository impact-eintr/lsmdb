package table

import (
	"bytes"
	"io"
	"math"
	"sort"

	"github.com/impact-eintr/lsmdb/y"
	"github.com/pkg/errors"
)

type blockIterator struct {
	data    []byte
	pos     uint32
	err     error
	baseKey []byte

	key  []byte
	val  []byte
	init bool

	last header // The last header we saw
}

func (itr *blockIterator) Reset() {
	itr.pos = 0
	itr.err = nil
	itr.baseKey = []byte{}
	itr.key = []byte{}
	itr.val = []byte{}
	itr.init = false
	itr.last = header{}
}

func (itr *blockIterator) Init() {
	if !itr.init {
		itr.Next()
	}
}

func (itr *blockIterator) Valid() bool {
	return itr != nil && itr.err == nil
}

func (itr *blockIterator) Error() error {
	return itr.err
}

func (itr *blockIterator) Close() {}

var (
	origin  = 0
	current = 1
)

// Seek brings us to the first block elemnet that is >= input key.
func (itr *blockIterator) Seek(key []byte, whence int) {
	itr.err = nil

	switch whence {
	case origin:
		itr.Reset()
	case current:
	}

	var done bool
	for itr.Init(); itr.Valid(); itr.Next() {
		k := itr.Key()
		if y.CompareKeys(k, key) >= 0 {
			// We are done as k is >= key
			done = true
			break
		}
	}
	if !done {
		itr.err = io.EOF
	}
}

func (itr *blockIterator) SeekToFirst() {
	itr.err = nil
	itr.Init()
}

func (itr *blockIterator) SeekToLast() {
	itr.err = nil
	for itr.Init(); itr.Valid(); itr.Next() {
	}
	itr.Prev()
}

func (itr *blockIterator) parseKV(h header) {
	if cap(itr.key) < int(h.plen+h.klen) {
		itr.key = make([]byte, 2*(h.plen+h.klen))
	}
	itr.key = itr.key[:h.plen+h.klen]
	copy(itr.key, itr.baseKey[:h.plen])
	copy(itr.key[h.plen:], itr.data[itr.pos:itr.pos+uint32(h.klen)])
	itr.pos += uint32(h.klen)

	if itr.pos+uint32(h.vlen) > uint32(len(itr.data)) {
		itr.err = errors.Errorf("Value exceeded size of block: %d %d %d %d %v",
			itr.pos, h.klen, h.vlen, len(itr.data), h)
		return
	}
	itr.val = y.Safecopy(itr.val, itr.data[itr.pos:itr.pos+uint32(h.vlen)])
	itr.pos += uint32(h.vlen)
}

func (itr *blockIterator) Next() {
	itr.init = true
	itr.err = nil
	if itr.pos >= uint32(len(itr.data)) {
		itr.err = io.EOF
		return
	}

	var h header
	itr.pos += uint32(h.Decode(itr.data[itr.pos:]))
	itr.last = h // store the last header

	if h.klen == 0 && h.plen == 0 {
		itr.err = io.EOF
		return
	}

	if len(itr.baseKey) == 0 {
		// This should be the first Next() for this block. Hence, prefix length should be zero.
		y.AssertTrue(h.plen == 0)
		itr.baseKey = itr.data[itr.pos : itr.pos+uint32(h.klen)]
	}
	itr.parseKV(h)
}

func (itr *blockIterator) Prev() {
	if !itr.init {
		return
	}
	itr.err = nil
	if itr.last.prev == math.MaxUint32 {
		itr.err = io.EOF
		itr.pos = 0
		return
	}

	itr.pos = itr.last.prev

	var h header
	y.AssertTruef(itr.pos < uint32(len(itr.data)), "%d %d", itr.pos, len(itr.data))
	itr.pos += uint32(h.Decode(itr.data[itr.pos:]))
	itr.parseKV(h)
	itr.last = h
}

func (itr *blockIterator) Key() []byte {
	if itr.err != nil {
		return nil
	}
	return itr.key
}

func (itr *blockIterator) Value() []byte {
	if itr.err != nil {
		return nil
	}
	return itr.val
}

// Iterator is an iterator for a Table.
type Iterator struct {
	t    *Table
	bpos int
	bi   *blockIterator
	err  error

	reversed bool
}

func (t *Table) NewIterator(reversed bool) *Iterator {
	t.IncrRef() // NOTE: Important
	ti := &Iterator{t: t, reversed: reversed}
	ti.next()
	return ti
}

func (itr *Iterator) Close() error {
	return itr.t.DecrRef()
}

func (itr *Iterator) reset() {
	itr.bpos = 0
	itr.err = nil
}

func (itr *Iterator) Valid() bool {
	return itr.err == nil
}

func (itr *Iterator) seekToFirst() {
	numBlockes := len(itr.t.blockIndex)
	if numBlockes == 0 {
		itr.err = io.EOF
		return
	}
	// 重新定位 Iterator 的 blockIterator
	itr.bpos = 0
	block, err := itr.t.block(itr.bpos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi = block.NewIterator()
	itr.bi.SeekToFirst()
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekToLast() {
	numBlockes := len(itr.t.blockIndex)
	if numBlockes == 0 {
		itr.err = io.EOF
		return
	}
	// 重新定位 Iterator 的 blockIterator
	itr.bpos = numBlockes - 1
	block, err := itr.t.block(itr.bpos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi = block.NewIterator()
	itr.bi.SeekToLast()
	itr.err = itr.bi.Error()
}

// 内置查找函数
func (itr *Iterator) seekHelper(blockIdx int, key []byte) {
	itr.bpos = blockIdx
	// 重新定位 Iterator 的 blockIterator
	block, err := itr.t.block(blockIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi = block.NewIterator()
	itr.bi.Seek(key, origin) // 用新的 blockIterator 定位 key
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekFrom(key []byte, whence int) {
	itr.err = nil
	switch whence { // whence 根源
	case origin:
		itr.reset()
	case current:
	}

	// 先检查缓存
	idx := sort.Search(len(itr.t.blockIndex), func(idx int) bool {
		ko := itr.t.blockIndex[idx]
		return y.CompareKeys(ko.key, key) > 0
	})
	if idx == 0 {
		itr.seekHelper(0, key)
		return
	}

	// TODO 下面的逻辑没看懂
	// block[idx].smallest is > key.
	// Since idx>0, we know block[idx-1].smallest is <= key.
	// There are two cases.
	// 1) Everything in block[idx-1] is strictly < key. In this case, we should go to the first
	//    element of block[idx].
	// 2) Some element in block[idx-1] is >= key. We should go to that element.
	itr.seekHelper(idx-1, key)
	if itr.err == io.EOF {
		// Case 1. Need to visit block[idx].
		if idx == len(itr.t.blockIndex) {
			// If idx == len(itr.t.blockIndex), then input key is greater than ANY element of table.
			// There's nothing we can do. Valid() should return false as we seek to end of table.
			return
		}
		// Since block[idx].smallest is > key. This is essentially a block[idx].SeekToFirst.
		itr.seekHelper(idx, key)
	}
	// Case 2: No need to do anything. We already did the seek in block[idx-1].
}

func (itr *Iterator) seek(key []byte) {
	itr.seekFrom(key, origin)
}

func (itr *Iterator) seekForPrev(key []byte) {
	itr.seekFrom(key, origin)
	if !bytes.Equal(itr.Key(), key) {
		itr.prev()
	}
}

func (itr *Iterator) next() {
	itr.err = nil
	if itr.bpos >= len(itr.t.blockIndex) {
		itr.err = io.EOF
		return
	}

	if itr.bi == nil {
		block, err := itr.t.block(itr.bpos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi = block.NewIterator()
		itr.bi.SeekToFirst()
		return
	}

	itr.bi.Next()
	if !itr.bi.Valid() { // 如果结果无效 继续往下走
		itr.bpos++
		itr.bi = nil
		itr.next()
		return
	}
}

func (itr *Iterator) prev() {
	itr.err = nil
	if itr.bpos < 0 {
		itr.err = io.EOF
		return
	}

	if itr.bi == nil {
		block, err := itr.t.block(itr.bpos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi = block.NewIterator()
		itr.bi.SeekToLast()
		return
	}

	itr.bi.Prev()
	if !itr.bi.Valid() { // 如果结果无效 继续往前走
		itr.bpos--
		itr.bi = nil
		itr.prev()
		return
	}
}

func (itr *Iterator) Key() []byte {
	return itr.bi.Key()
}

func (itr *Iterator) Value() (ret y.ValueStruct) {
	ret.Decode(itr.bi.Value())
	return
}

func (itr *Iterator) Next() {
	if !itr.reversed {
		itr.next()
	} else {
		itr.prev()
	}
}

func (itr *Iterator) Rewind() {
	if !itr.reversed {
		itr.seekToFirst()
	} else {
		itr.seekToLast()
	}
}

func (itr *Iterator) Seek(key []byte) {
	if !itr.reversed {
		itr.seek(key)
	} else {
		itr.seekForPrev(key)
	}
}

// ConcatIterator 连接由多个迭代器定义的序列。 （它只适用于 TableIterators，可能只是因为它不那么通用更快。）
// Concat: 合并多个数组；合并多个字符串
type ConcatIterator struct {
	idx      int // Which iterator is active now
	cur      *Iterator
	iters    []*Iterator // Corresponds(对应) the tables
	tables   []*Table    // Disregarding(忽视) reversed, thsi is in ascending order(升序)
	reversed bool
}

func NewConcatIterator(tbls []*Table, reversed bool) *ConcatIterator {
	iters := make([]*Iterator, len(tbls))
	for i := 0; i < len(tbls); i++ {
		iters[i] = tbls[i].NewIterator(reversed)
	}
	return &ConcatIterator{
		reversed: reversed,
		iters:    iters,
		tables:   tbls,
		idx:      -1, // Not really necessary because s.it.Valid()=false, but good to have.
	}
}

func (s *ConcatIterator) setIdx(idx int) {
	s.idx = idx
	if idx < 0 || idx >= len(s.iters) {
		s.cur = nil
	} else {
		s.cur = s.iters[s.idx]
	}
}

func (s *ConcatIterator) Rewind() {
	if len(s.iters) == 0 {
		return
	}
	if !s.reversed {
		s.setIdx(0)
	} else {
		s.setIdx(len(s.iters) - 1)
	}
	s.cur.Rewind()
}

func (s *ConcatIterator) Valid() bool {
	return s.cur != nil && s.cur.Valid()
}

func (s *ConcatIterator) Key() []byte {
	return s.cur.Key()
}

func (s *ConcatIterator) Value() y.ValueStruct {
	return s.cur.Value()
}

func (s *ConcatIterator) Seek(key []byte) {
	var idx int
	if !s.reversed {
		idx = sort.Search(len(s.tables), func(i int) bool {
			return y.CompareKeys(s.tables[i].Biggest(), key) >= 0
		})
	} else {
		n := len(s.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return y.CompareKeys(s.tables[n-1-i].Smallest(), key) <= 0
		})
	}
	if idx >= len(s.tables) || idx < 0 {
		s.setIdx(-1)
		return
	}
	// For reversed=false, we know s.tables[i-1].Biggest() < key. Thus, the
	// previous table cannot possibly contain key.
	s.setIdx(idx)
	s.cur.Seek(key)
}

func (s *ConcatIterator) Next() {
	s.cur.Next()
	if s.cur.Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		if !s.reversed {
			s.setIdx(s.idx + 1)
		} else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			// End of list. Valid will become false.
			return
		}
		s.cur.Rewind()
		if s.cur.Valid() {
			break
		}
	}
}

func (s *ConcatIterator) Close() error {
	for _, it := range s.iters {
		if err := it.Close(); err != nil {
			return errors.Wrap(err, "ConcatIterator")
		}
	}
	return nil
}
