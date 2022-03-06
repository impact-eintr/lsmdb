package lsmdb

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/impact-eintr/lsmdb/y"
)

// 预先载入
type prefetchStatus uint8

const (
	prefetched prefetchStatus = iota + 1
)

// Item is returned during iteration. Both the Key() and Value() output is only valid until
// iterator.Next() is called.
type Item struct {
	status    prefetchStatus
	err       error
	wg        sync.WaitGroup
	db        *DB
	key       []byte
	vptr      []byte
	meta      byte // we need to store meta to know about bitValuePointer.
	userMeta  byte
	expiresAt uint64
	val       []byte
	slice     *y.Slice
	next      *Item
	version   uint64
	txn       *Txn
}

// ToString returns a string representation of Item
func (item *Item) ToString() string {
	return fmt.Sprintf("key=%q, version=%d, meta=%x", item.Key(), item.Version(), item.meta)

}

// Key returns the key.
//
// Key is only valid as long as item is valid, or transaction is valid.  If you need to use it
// outside its validity, please copy it.
func (item *Item) Key() []byte {
	return item.key
}

// Version returns the commit timestamp of the item.
func (item *Item) Version() uint64 {
	return item.version
}

// Value retrieves the value of the item from the value log.
//
// The returned value is only valid as long as item is valid, or transaction is valid. So, if you
// need to use it outside, please parse or copy it.
func (item *Item) Value() ([]byte, error) {
	item.wg.Wait()
	if item.status == prefetched {
		return item.val, item.err
	}
	buf, cb, err := item.yieldItemValue()
	if cb != nil {
		item.txn.callbacks = append(item.txn.callbacks, cb)
	}
	return buf, err
}

func (item *Item) hasValue() bool {
	if item.meta == 0 && item.vptr == nil {
		// key not found
		return false
	}
	return true
}

func (item *Item) yieldItemValue() ([]byte, func(), error) {
	if !item.hasValue() {
		return nil, nil, nil
	}

	if item.slice == nil {
		item.slice = new(y.Slice)
	}

	// 直接保存了值
	if (item.meta & bitValuePointer) == 0 {
		val := item.slice.Resize(len(item.vptr))
		copy(val, item.vptr)
		return val, nil, nil
	}

	// 保存的是值的指针
	var vp valuePointer
	vp.Decode(item.vptr)
	return item.db.vlog.Read(vp)
}

func runCallback(cb func()) {
	if cb != nil {
		cb()
	}
}

func (item *Item) prefetchValue() {
	val, cb, err := item.yieldItemValue()
	defer runCallback(cb)

	item.err = err
	item.status = prefetched
	if val == nil {
		return
	}
	buf := item.slice.Resize(len(val))
	copy(buf, val)
	item.val = buf
}

// EstimatedSize returns approximate size of the key-value pair.
//
// This can be called while iterating through a store to quickly estimate the
// size of a range of key-value pairs (without fetching the corresponding
// values).
func (item *Item) EstimatedSize() int64 {
	if !item.hasValue() {
		return 0
	}
	if (item.meta & bitValuePointer) == 0 {
		return int64(len(item.key) + len(item.vptr))
	}
	var vp valuePointer
	vp.Decode(item.vptr)
	return int64(vp.Len) // includes key length.
}

// UserMeta returns the userMeta set by the user. Typically, this byte, optionally set by the user
// is used to interpret the value.
func (item *Item) UserMeta() byte {
	return item.userMeta
}

// ExpiresAt returns a Unix time value indicating when the item will be
// considered expired. 0 indicates that the item will never expire.
func (item *Item) ExpiresAt() uint64 {
	return item.expiresAt
}

type list struct {
	head *Item
	tail *Item
}

func (l *list) push(i *Item) {
	i.next = nil
	if l.tail == nil {
		l.head = i
		l.tail = i
		return
	}
	l.tail.next = i
	l.tail = i
}

func (l *list) pop() *Item {
	if l.head == nil {
		return nil
	}
	i := l.head
	if l.head == l.tail {
		l.tail = nil
		l.head = nil
	} else {
		l.head = i.next
	}
	i.next = nil
	return i
}

// IteratorOptions is used to set options when iterating over lsmdb key-value stores.
//
// This package provides DefaultIteratorOptions which contains options that
// should work for most applications. Consider using that as a starting point
// before customizing it for your own needs.
type IteratorOptions struct {
	// Indicates whether we should prefetch values during iteration and store them.
	PrefetchValues bool
	// How many KV pairs to prefetch while iterating. Valid only if PrefetchValues is true.
	PrefetchSize int
	Reverse      bool // Direction of iteration. False is forward, true is backward.
	AllVersions  bool // Fetch all valid versions of the same key.
}

// DefaultIteratorOptions contains default options when iterating over lsmdb key-value stores.
var DefaultIteratorOptions = IteratorOptions{
	PrefetchValues: true,
	PrefetchSize:   100,
	Reverse:        false,
	AllVersions:    false,
}

// Iterator helps iterating over the KV pairs in a lexicographically(按字典顺序) sorted order.
type Iterator struct {
	iitr   *y.MergeIterator
	txn    *Txn
	readTs uint64

	opt   IteratorOptions
	item  *Item
	data  list
	waste list

	lastKey []byte // Used to skip over multiple versions of the same key.
}

// NewIterator returns a new iterator. Depending upon(依赖于) the options, either only keys, or both
// key-value pairs would be fetched. The keys are returned in lexicographically(按照字典序) sorted order.
// Using prefetch is highly recommended if you're doing a long running iteration.
// Avoid long running iterations in update transactions.
func (txn *Txn) NewIterator(opt IteratorOptions) *Iterator {
	tables, decr := txn.db.getMemTables() // 返回 mutable/immutable memtable
	defer decr()
	txn.db.vlog.incrIteratorCount()
	var iters []y.Iterator
	for i := 0; i < len(tables); i++ {
		iters = append(iters, tables[i].NewUniIterator(opt.Reverse))
	}
	iters = txn.db.lc.appendIterators(iters, opt.Reverse) // This will increment references.
	res := &Iterator{
		txn:    txn,
		iitr:   y.NewMergeIterator(iters, opt.Reverse),
		opt:    opt,
		readTs: txn.readTs,
	}
	return res
}

func (it *Iterator) newItem() *Item {
	item := it.waste.pop()
	if item == nil {
		item = &Item{slice: new(y.Slice), db: it.txn.db, txn: it.txn}
	}
	return item
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
func (it *Iterator) Item() *Item {
	tx := it.txn
	if tx.update {
		// Track reads if this is an update txn.
		tx.reads = append(tx.reads, farm.Fingerprint64(it.item.Key()))
	}
	return it.item
}

// Valid returns false when iteration is done.
func (it *Iterator) Valid() bool { return it.item != nil }

// ValidForPrefix returns false when iteration is done
// or when the current key is not prefixed by the specified prefix.
func (it *Iterator) ValidForPrefix(prefix []byte) bool {
	return it.item != nil && bytes.HasPrefix(it.item.key, prefix)
}

// Close would close the iterator. NOTE: It is important to call this when you're done with iteration.
func (it *Iterator) Close() {
	it.iitr.Close()
	// TODO: We could handle this error.
	_ = it.txn.db.vlog.decrIteratorCount()
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
func (it *Iterator) Next() {
	// Reuse current item
	it.item.wg.Wait() // Just cleaner to wait before pushing to avoid doing ref counting.
	it.waste.push(it.item)

	// Set next item to current
	it.item = it.data.pop()

	for it.iitr.Valid() {
		if it.parseItem() {
			// parseItem calls one extra next.
			// This is used to deal with the complexity of reverse iteration.
			break
		}
	}
}

func isDeletedOrExpired(vs y.ValueStruct) bool {
	if vs.Meta&bitDelete > 0 {
		return true
	}
	if vs.ExpiresAt == 0 {
		return false
	}
	return vs.ExpiresAt <= uint64(time.Now().Unix())
}

// Iterator.Next() calls parseItem
func (it *Iterator) parseItem() bool {

}

func (it *Iterator) fill(item *Item) {

}

func (it *Iterator) prefetch() {

}

func (it *Iterator) Seek(key []byte) {

}

func (it *Iterator) Rewind() {

}
