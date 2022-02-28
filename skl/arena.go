package skl

import (
	"sync/atomic"
	"unsafe"

	"github.com/impact-eintr/lsmdb/y"
)

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))
	ptrAlign   = int(unsafe.Sizeof(uintptr(0))) - 1
)

type Arena struct {
	n   uint32
	buf []byte
}

func newArena(n int64) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind of nil pointer.
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func (s *Arena) reset() {
	atomic.StoreUint32(&s.n, 0)
}

// putNode 在 Arena 中分配一个节点。该节点在指针大小的边界上对齐。返回节点的 Arena 偏移量。
func (s *Arena) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + ptrAlign)
	n := atomic.AddUint32(&s.n, l)
	y.AssertTruef(int(n) <= len(s.buf),
		"Arena too small, toWrite:%d newTotal:%d limit:%d",
		l, n, len(s.buf))

	// Return the aligned offset.
	m := (n - l + uint32(ptrAlign)) & ^uint32(ptrAlign)
	return m
}

func (s *Arena) putVal(v y.ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	n := atomic.AddUint32(&s.n, l)
	y.AssertTruef(int(n) <= len(s.buf),
		"Arena too small, toWrite:%d newTotal:%d limit:%d",
		l, n, len(s.buf))
	m := n - l
	v.Encode(s.buf[m:])
	return m
}

func (s *Arena) putKey(key []byte) uint32 {
	l := uint32(len(key))
	n := atomic.AddUint32(&s.n, l)
	y.AssertTruef(int(n) <= len(s.buf),
		"Arena too small, toWrite:%d newTotal:%d limit:%d",
		l, n, len(s.buf))
	m := n - l
	y.AssertTrue(len(key) == copy(s.buf[m:n], key))
	return m
}

func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint16) (ret y.ValueStruct) {
	ret.Decode(s.buf[offset : offset+uint32(size)])
	return
}

func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}
