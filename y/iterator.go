package y

import (
	"bytes"
	"container/heap"
	"encoding/binary"

	"github.com/pkg/errors"
)

type ValueStruct struct {
	Meta      byte
	UserMeta  byte
	ExpiresAt uint64
	Value     []byte

	Version uint64 // This field is not serialized. Only for internal usage.
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func (v *ValueStruct) EncodedSize() uint16 {
	sz := len(v.Value) + 2 // meta, usermeta
	if v.ExpiresAt == 0 {
		return uint16(sz + 1)
	}
	enc := sizeVarint(v.ExpiresAt)
	return uint16(sz + enc)
}

func (v *ValueStruct) Decode(b []byte) {
	v.Meta = b[0]
	v.UserMeta = b[1]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(b[2:])
	v.Value = b[2+sz:]
}

func (v *ValueStruct) Encode(b []byte) {
	b[0] = v.Meta
	b[1] = v.UserMeta
	sz := binary.PutUvarint(b[2:], v.ExpiresAt)
	copy(b[2+sz:], v.Value)
}

func (v *ValueStruct) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(v.Meta)
	buf.WriteByte(v.UserMeta)
	var enc [binary.MaxVarintLen64]byte
	sz := binary.PutUvarint(enc[:], v.ExpiresAt)
	buf.Write(enc[:sz])
	buf.Write(v.Value)
}

type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() ValueStruct
	Valid() bool
	Close() error // All iterators should be closed so that file garbage collection works
}

type elem struct {
	itr      Iterator
	nice     int
	reversed bool
}

type elemHeap []*elem

func (eh elemHeap) Len() int      { return len(eh) }
func (eh elemHeap) Swap(i, j int) { eh[i], eh[j] = eh[j], eh[i] }
func (eh elemHeap) Less(i, j int) bool {
	cmp := CompareKeys(eh[i].itr.Key(), eh[j].itr.Key())
	if cmp < 0 {
		return !eh[i].reversed
	}
	if cmp > 0 {
		return eh[i].reversed
	}
	// The keys are equal. In this case, lower nice take precedence. This is important.
	return eh[i].nice < eh[j].nice
}
func (eh *elemHeap) Push(x interface{}) { *eh = append(*eh, x.(*elem)) }
func (eh *elemHeap) Pop() interface{} {
	old := *eh
	n := len(old)
	x := old[n-1]
	*eh = old[0 : n-1]
	return x
}

// MergeIterator merges multiple iterators.
type MergeIterator struct {
	h        elemHeap
	curKey   []byte
	reversed bool

	all []Iterator
}

func NewMergeIterator(iters []Iterator, reversed bool) *MergeIterator {
	m := &MergeIterator{all: iters, reversed: reversed}
	m.h = make(elemHeap, 0, len(iters))
	m.initHeap()
	return m
}

// 将smallest的Key 复制到mergeIterator的curKey
func (s *MergeIterator) storeKey(smallest Iterator) {
	if cap(s.curKey) < len(smallest.Key()) {
		s.curKey = make([]byte, 2*len(smallest.Key()))
	}
	s.curKey = s.curKey[:len(smallest.Key())]
	copy(s.curKey, smallest.Key())
}

// initHeap 检查所有迭代器并初始化我们的堆和键数组
// 每当我们反转方向时，我们都需要运行这个
func (s *MergeIterator) initHeap() {
	s.h = s.h[:0]
	for idx, itr := range s.all {
		if !itr.Valid() {
			continue
		}
		e := &elem{itr: itr, nice: idx, reversed: s.reversed}
		s.h = append(s.h, e)
	}
	heap.Init(&s.h)
	for len(s.h) > 0 {
		it := s.h[0].itr
		if it == nil || !it.Valid() {
			heap.Pop(&s.h)
			continue
		}
		s.storeKey(s.h[0].itr)
		break
	}
}

func (s *MergeIterator) Valid() bool {
	if s == nil || len(s.h) == 0 {
		return false
	}
	return s.h[0].itr.Valid()
}

func (s *MergeIterator) Key() []byte {
	if len(s.h) == 0 {
		return nil
	}
	return s.h[0].itr.Key()
}

func (s *MergeIterator) Value() ValueStruct {
	if len(s.h) == 0 {
		return ValueStruct{}
	}
	return s.h[0].itr.Value()
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (s *MergeIterator) Next() {
	if len(s.h) == 0 {
		return
	}

	smallest := s.h[0].itr
	smallest.Next()

	for len(s.h) > 0 {
		smallest = s.h[0].itr
		if !smallest.Valid() {
			heap.Pop(&s.h)
			continue
		}

		heap.Fix(&s.h, 0)
		smallest = s.h[0].itr
		if smallest.Valid() {
			if !bytes.Equal(smallest.Key(), s.curKey) {
				break
			}
			smallest.Next()
		}
	}
	if !smallest.Valid() {
		return
	}
	s.storeKey(smallest)
}

// Rewind 寻找第一个元素（或反向迭代器的最后一个元素）
func (s *MergeIterator) Rewind() {
	for _, itr := range s.all {
		itr.Rewind()
	}
	s.initHeap()
}

// Seek 将我们带到带有键 >= 给定键的元素
func (s *MergeIterator) Seek(key []byte) {
	for _, itr := range s.all {
		itr.Seek(key)
	}
	s.initHeap()
}

func (s *MergeIterator) Close() error {
	for _, itr := range s.all {
		if err := itr.Close(); err != nil {
			return errors.Wrap(err, "MergeIterator")
		}
	}
	return nil
}
