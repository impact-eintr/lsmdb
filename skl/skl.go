package skl

import (
	"math"
	"math/rand"
	"sync/atomic"
	"unsafe"

	"github.com/impact-eintr/lsmdb/y"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

const MaxNodeSize = int(unsafe.Sizeof(node{}))

type node struct {
	//  TODO 一个24字节长的[]byte。我们试图在这里节省空间。
	keyOffset uint32 // 不可变。无需锁定访问密钥
	keySize   uint16 // 不可变。无需锁定访问密钥

	// 塔的高度
	height uint16

	// 值的多个部分被编码为单个 uint64，以便可以原子地加载和存储它
	//  value offset: uint32(bits 0~31)
	//  value size  : uint16(bits 32~47)
	value uint64

	// 大多数节点不需要使用塔的整个高度，因为每个连续级别的概率呈指数下降。
	// 因为这些元素永远不会被访问，所以它们不需要被分配。
	// 因此，当一个节点在 arena 中被分配时，它的内存占用被故意截断以不包括不需要的塔元素。
	//
	// 所有对元素的访问都应使用 CAS 操作，无需锁定。
	tower [maxHeight]uint32
}

// Skiplist maps keys to values (in memory)
type Skiplist struct {
	height int32
	head   *node
	ref    int32
	arena  *Arena
}

func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}

	s.arena.reset()
	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skiplist when we are supposed to have no reference!
	s.arena = nil
}

func (s *Skiplist) valid() bool { return s.arena != nil }

func newNode(arena *Arena, key []byte, v y.ValueStruct, height int) *node {
	// The base level is already allocated in the node struct.
	offset := arena.putNode(height)
	node := arena.getNode(offset)
	node.keyOffset = arena.putKey(key)
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = encodeValue(arena.putVal(v), v.EncodedSize())
	return node
}

func encodeValue(valOffset uint32, valSize uint16) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint16) {
	valOffset = uint32(value)
	valSize = uint16(value >> 32)
	return
}

// NewSkiplist makes a new empty skiplist, with a given arena size
func NewSkiplist(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, y.ValueStruct{}, maxHeight)
	return &Skiplist{
		height: 1,
		head:   head,
		arena:  arena,
		ref:    1,
	}
}

func (s *node) getValueOffset() (uint32, uint16) {
	value := atomic.LoadUint64(&s.value)
	return decodeValue(value)
}

func (s *node) key(arena *Arena) []byte {
	return arena.getKey(s.keyOffset, s.keySize)
}

func (s *node) setValue(arena *Arena, v y.ValueStruct) {
	valOffset := arena.putVal(v)
	value := encodeValue(valOffset, v.EncodedSize())
	atomic.StoreUint64(&s.value, value)
}

func (s *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&s.tower[h])
}

func (s *node) casNextOffset(h int, old, val uint32) bool {
	// CAS指令执行时，当且仅当内存地址V的值与预期值A相等时，将内存地址V的值修改为B，否则就什么都不做。
	// 整个比较并替换的操作是一个原子操作。
	return atomic.CompareAndSwapUint32(&s.tower[h], old, val)
}

func randomHeight() int {
	h := 1
	for h < maxHeight && rand.Uint32() <= heightIncrease { // 1/3的概率
		h++
	}
	return h
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

// findNear 查找键附近的节点。
// 如果less=true，它会找到最右边的节点，例如node.key < key（如果allowEqual=false）
// 或node.key <= key（如果allowEqual=true）。
// 如果less=false，它会找到最左边的节点，例如node.key > key（如果allowEqual=false）
// 或node.key >= key（如果allowEqual=true）。
// 返回找到的节点。如果节点的键等于给定键，则返回的布尔值为真。
func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.head
	level := int(s.getHeight() - 1)
	for {
		// 假设 x.key < key
		next := s.getNext(x, level)
		if next == nil {
			// x.key < key < END OF LIST
			if level > 0 {
				level--
				continue
			}
			// Levle == 0
			if !less {
				return nil, false
			}
			// Try to return x.Make sure it is not a head node.
			if x == s.head {
				return nil, false
			}
			return x, false
		}

		nextKey := next.key(s.arena)
		cmp := y.CompareKeys(key, nextKey)
		if cmp > 0 {
			// x.key < next.key < key
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key
			if allowEqual {
				return next, true
			}
			if !less {
				// we want >,so go to base level to grab the next bigger note.
				return s.getNext(next, 0), false
			}
			// We want <.If not base level, we should go closer in the next level
			if level > 0 {
				level--
				continue
			}
			// On base level.Return x.
			if x == s.head {
				return nil, false
			}
			return x, false
		}
		// cmp < 0 即 x.key < key < next.key
		if level > 0 {
			level--
			continue
		}
		if !less {
			return next, false
		}
		if x == s.head {
			return nil, false
		}
		return x, false
	}
}

// TODO 这个函数是在干什么
// findSpliceForLevel 返回 (outBefore, outAfter)，其中 outBefore.key <= key <= outAfter.key。
// 输入“之前”告诉我们从哪里开始查找。
// 如果我们找到具有相同键的节点，则返回 outBefore = outAfter。
// 否则，outBefore.key < key < outAfter.key。
func (s *Skiplist) findSpliceForLevel(key []byte, before *node, level int) (*node, *node) {
	for {
		// Assume before.key < key.
		next := s.getNext(before, level)
		if next == nil {
			return before, next
		}
		nextKey := next.key(s.arena)
		cmp := y.CompareKeys(key, nextKey)
		if cmp == 0 {
			// Equality case.
			return next, next
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next
		}
		before = next // Keep moving right on this level.
	}
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// Put inserts the key-value pair
func (s *Skiplist) Put(key []byte, v y.ValueStruct) {
	listHeight := s.getHeight()
	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node
	prev[listHeight] = s.head
	next[listHeight] = nil
	for i := int(listHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			prev[i].setValue(s.arena, v)
			return
		}
	}

	// we do need to create a new node
	height := randomHeight() // 按照概率建立索引
	x := newNode(s.arena, key, v, height)

	// Try to increase s.height via CAS.
	listHeight = s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			// Successfully increased skiplist.height.
			break
		}
		listHeight = s.getHeight()
	}
	//log.Println("重新建立索引", height, atomic.LoadInt32(&s.height))

	// 我们总是从基础级别和向上插入。
	// 在基础级别添加节点后，我们无法在上面的级别创建节点，因为它会发现基础级别中的节点
	for i := 0; i < height; i++ {
		for {
			if prev[i] == nil {
				y.AssertTrue(i > 1) // This cannot happen in base level.
				// We haven't computed prev, next for this level because height exceeds old listHeight.
				// For these levels, we expect the lists to be sparse(稀疏的), so we can just search from head.
				prev[i], next[i] = s.findSpliceForLevel(key, s.head, i)
				// Someone adds the exact same key before we are able to do so. This can only happen on
				// the base level. But we know we are not on the base level.
				y.AssertTrue(prev[i] != next[i])
			}
			nextOffset := s.arena.getNodeOffset(next[i])
			x.tower[i] = nextOffset
			if prev[i].casNextOffset(i, nextOffset, s.arena.getNodeOffset(x)) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				y.AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				prev[i].setValue(s.arena, v)
				return
			}
		}
	}
}

func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

// findLast returns the last element. If head (empty list), we return nil.
// All the find functions will NEVER return the head nodes.
func (s *Skiplist) findLast() *node {
	n := s.head
	level := int(s.getHeight() - 1)
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.head {
				return nil
			}
			return n
		}
		level--
	}
}

// Get 获取与键关联的值。如果它找到相同的或较早版本的相同键，它会返回一个有效值。
func (s *Skiplist) Get(key []byte) y.ValueStruct {
	n, _ := s.findNear(key, false, true)
	if n == nil {
		return y.ValueStruct{}
	}

	nextKey := s.arena.getKey(n.keyOffset, n.keySize)
	if !y.SameKey(key, nextKey) {
		return y.ValueStruct{}
	}

	valOffset, valSize := n.getValueOffset()
	vs := s.arena.getVal(valOffset, valSize)
	vs.Version = y.ParseTs(nextKey)
	return vs
}

func (s *Skiplist) NewIterator() *Iterator {
	s.IncrRef()
	return &Iterator{list: s}
}

func (s *Skiplist) MemSize() int64 { return s.arena.size() }

type Iterator struct {
	list *Skiplist
	n    *node
}

func (s *Iterator) Close() error {
	s.list.DecrRef()
	return nil
}

func (s *Iterator) Valid() bool { return s.n != nil }

func (s *Iterator) Key() []byte {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

// Value returns value.
func (s *Iterator) Value() y.ValueStruct {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.arena.getVal(valOffset, valSize)
}

// Next advances to the next position.
func (s *Iterator) Next() {
	y.AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

// Prev advances to the previous position.
func (s *Iterator) Prev() {
	y.AssertTrue(s.Valid())
	s.n, _ = s.list.findNear(s.Key(), true, false) // find <. No equality allowed.
}

// Seek advances to the first Entry with a key >= target.
func (s *Iterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true) // find >=.
}

// SeekForPrev finds an Entry with key <= target.
func (s *Iterator) SeekForPrev(target []byte) {
	s.n, _ = s.list.findNear(target, true, true) // find <=.
}

// SeekToFirst seeks position at the first Entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.head, 0)
}

// SeekToLast seeks position at the last Entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToLast() {
	s.n = s.list.findLast()
}

// UniIterator is a unidirectional memtable iterator. It is a thin wrapper around
// Iterator. We like to keep Iterator as before, because it is more powerful and
// we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *Iterator
	reversed bool
}

// NewUniIterator returns a UniIterator.
func (s *Skiplist) NewUniIterator(reversed bool) *UniIterator {
	return &UniIterator{
		iter:     s.NewIterator(),
		reversed: reversed,
	}
}

// Next implements y.Interface
func (s *UniIterator) Next() {
	if !s.reversed {
		s.iter.Next()
	} else {
		s.iter.Prev()
	}
}

// Rewind implements y.Interface
func (s *UniIterator) Rewind() {
	if !s.reversed {
		s.iter.SeekToFirst()
	} else {
		s.iter.SeekToLast()
	}
}

// Seek implements y.Interface
func (s *UniIterator) Seek(key []byte) {
	if !s.reversed {
		s.iter.Seek(key)
	} else {
		s.iter.SeekForPrev(key)
	}
}

// Key implements y.Interface
func (s *UniIterator) Key() []byte { return s.iter.Key() }

// Value implements y.Interface
func (s *UniIterator) Value() y.ValueStruct { return s.iter.Value() }

// Valid implements y.Interface
func (s *UniIterator) Valid() bool { return s.iter.Valid() }

// Close implements y.Interface (and frees up the iter's resources)
func (s *UniIterator) Close() error { return s.iter.Close() }
