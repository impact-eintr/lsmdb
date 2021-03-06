package lsmdb

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/impact-eintr/lsmdb/y"
	"github.com/pkg/errors"
)

type uint64Heap []uint64

func (u uint64Heap) Len() int               { return len(u) }
func (u uint64Heap) Less(i int, j int) bool { return u[i] < u[j] }
func (u uint64Heap) Swap(i int, j int)      { u[i], u[j] = u[j], u[i] }
func (u *uint64Heap) Push(x interface{})    { *u = append(*u, x.(uint64)) }
func (u *uint64Heap) Pop() interface{} {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

type oracle struct {
	isManaged bool // Does not change value, so no locking required.

	sync.Mutex
	curRead    uint64
	nextCommit uint64

	// These two structures are used to figure out(确定) when a commit is done. The minimum done commit is
	// used to update curRead.
	// 这两个结构用于确定何时完成提交 最小完成提交用于更新 curRead
	commitMark     uint64Heap
	pendingCommits map[uint64]struct{}

	// commits stores a key fingerprint(指纹) and latest commit counter for it.
	// refCount is used to clear out(清理) commits map to avoid a memory blowup(崩溃).
	commits  map[uint64]uint64 // ts transaction
	refCount int64
}

func (o *oracle) addRef() {
	atomic.AddInt64(&o.refCount, 1)
}

func (o *oracle) decrRef() {
	if count := atomic.AddInt64(&o.refCount, -1); count == 0 {
		// Clear out pendingCommits maps to release memory.
		o.Lock()
		// There could be race here, so check again.
		// Checking commitMark is safe since it is protected by mutex.
		if len(o.commitMark) > 0 {
			o.Unlock()
			return
		}
		y.AssertTrue(len(o.pendingCommits) == 0)
		if len(o.commits) >= 1000 { // If the map is still small, let it slide.
			o.commits = make(map[uint64]uint64)
		}
		o.Unlock()
	}
}

func (o *oracle) readTs() uint64 {
	if o.isManaged {
		return math.MaxUint64
	}
	return atomic.LoadUint64(&o.curRead)
}

func (o *oracle) commitTs() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.nextCommit
}

// hasConflict(冲突) must be called while having a lock.
func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.reads) == 0 {
		return false
	}
	for _, ro := range txn.reads {
		if ts, has := o.commits[ro]; has && ts > txn.readTs {
			return true
		}
	}
	return false
}

func (o *oracle) newCommitTs(txn *Txn) uint64 {
	o.Lock()
	defer o.Unlock()

	if o.hasConflict(txn) {
		return 0
	}

	var ts uint64
	if !o.isManaged {
		// This is the general case, when user doesn't specify the read and commit ts.
		ts = o.nextCommit
		o.nextCommit++

	} else {
		// If commitTs is set, use it instead.
		ts = txn.commitTs
	}

	for _, w := range txn.writes {
		o.commits[w] = ts // Update the commitTs.
	}
	if o.isManaged {
		// No need to update the heap.
		return ts
	}
	heap.Push(&o.commitMark, ts)
	if _, has := o.pendingCommits[ts]; has {
		panic(fmt.Sprintf("We shouldn't have the commit ts: %d", ts))
	}
	o.pendingCommits[ts] = struct{}{}

	return ts
}

func (o *oracle) doneCommit(cts uint64) {
	if o.isManaged {
		// No need to update anything.
		return
	}
	o.Lock()
	defer o.Unlock()

	if _, has := o.pendingCommits[cts]; !has {
		panic(fmt.Sprintf("We should already have the commit ts: %d", cts))
	}
	delete(o.pendingCommits, cts)

	var min uint64
	for len(o.commitMark) > 0 {
		ts := o.commitMark[0]
		if _, has := o.pendingCommits[ts]; has {
			// Still waiting for a txn to commit.
			break
		}
		min = ts
		heap.Pop(&o.commitMark)
	}
	if min == 0 {
		return
	}
	atomic.StoreUint64(&o.curRead, min)
	// nextCommit must never be reset.
}

// Txn represents a lsmdb transaction.
type Txn struct {
	readTs   uint64
	commitTs uint64

	update bool     // update is used to conditionally keep track of reads.
	reads  []uint64 // contains fingerprints of keys read.
	writes []uint64 // contains fingerprints of keys written.

	pendingWrites map[string]*Entry // cache stores any writes done by txn.

	db        *DB
	callbacks []func()
	discarded bool

	size  int64
	count int64
}

func (txn *Txn) checkSize(e *Entry) error {
	count := txn.count + 1
	size := txn.size + int64(e.estimateSize(txn.db.opt.ValueThreshold)) + 10 // Extra bytes for version in key.
	if count >= txn.db.opt.maxBatchCount || size >= txn.db.opt.maxBatchSize {
		return ErrTxnTooBig
	}
	txn.count, txn.size = count, size
	return nil
}

// Set adds a key-value pair to the database.
//
// It will return ErrReadOnlyTxn if update flag was set to false when creating the
// transaction.
func (txn *Txn) Set(key, val []byte) error {
	e := &Entry{
		Key:   key,
		Value: val,
	}
	return txn.setEntry(e)
}

// Update delete a key-value pair then add it again with new value
func (txn *Txn) Update(key, val []byte) error {
	err := txn.Delete(key)
	if err != nil {
		return err
	}
	return txn.Set(key, val)
}

// SetWithMeta adds a key-value pair to the database, along with a metadata
// byte. This byte is stored alongside the key, and can be used as an aid to
// interpret the value or store other contextual bits corresponding to the
// key-value pair.
func (txn *Txn) SetWithMeta(key, val []byte, meta byte) error {
	e := &Entry{Key: key, Value: val, UserMeta: meta}
	return txn.setEntry(e)
}

// SetWithTTL adds a key-value pair to the database, along with a time-to-live
// (TTL) setting. A key stored with with a TTL would automatically expire after
// the time has elapsed , and be eligible for garbage collection.
func (txn *Txn) SetWithTTL(key, val []byte, dur time.Duration) error {
	expire := time.Now().Add(dur).Unix()
	e := &Entry{Key: key, Value: val, ExpiresAt: uint64(expire)}
	return txn.setEntry(e)
}

func (txn *Txn) setEntry(e *Entry) error {
	switch {
	case !txn.update:
		return ErrReadOnlyTxn
	case txn.discarded:
		return ErrDiscardedTxn
	case len(e.Key) == 0:
		return ErrEmptyKey
	case len(e.Key) > maxKeySize:
		return exceedsMaxKeySizeError(e.Key)
	case int64(len(e.Value)) > txn.db.opt.ValueLogFileSize:
		return exceedsMaxValueSizeError(e.Value, txn.db.opt.ValueLogFileSize)
	}
	if err := txn.checkSize(e); err != nil {
		return err
	}

	fp := farm.Fingerprint64(e.Key) // Avoid dealing with byte arrays.
	txn.writes = append(txn.writes, fp)
	txn.pendingWrites[string(e.Key)] = e
	//log.Println("before commit set ", e.Value)
	return nil
}

// Delete deletes a key. This is done by adding a delete marker for the key at commit timestamp.
// Any reads happening before this timestamp would be unaffected. Any reads after this commit would
// see the deletion.
func (txn *Txn) Delete(key []byte) error {
	if !txn.update {
		return ErrReadOnlyTxn
	} else if txn.discarded {
		return ErrDiscardedTxn
	} else if len(key) == 0 {
		return ErrEmptyKey
	} else if len(key) > maxKeySize {
		return exceedsMaxKeySizeError(key)
	}

	e := &Entry{
		Key:  key,
		meta: bitDelete,
	}
	if err := txn.checkSize(e); err != nil {
		return err
	}

	fp := farm.Fingerprint64(key) // Avoid dealing with byte arrays.
	txn.writes = append(txn.writes, fp)

	txn.pendingWrites[string(key)] = e
	return nil
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	} else if txn.discarded {
		return nil, ErrDiscardedTxn
	}

	item = new(Item)
	if txn.update {
		if e, has := txn.pendingWrites[string(key)]; has && bytes.Equal(key, e.Key) {
			if e.meta&bitDelete > 0 {
				return nil, ErrKeyNotFound
			}
			if e.ExpiresAt > 0 && e.ExpiresAt <= uint64(time.Now().Unix()) {
				return nil, ErrKeyNotFound
			}
			// Fulfill from cache.
			item.meta = e.meta
			item.val = e.Value
			item.userMeta = e.UserMeta
			item.key = e.Key
			item.status = prefetched
			item.version = txn.readTs
			// We probably don't need to set db on item here
			return item, nil
		}
		// Only track reads if this is update txn. No need to track read if txn serviced it
		// internally.
		fp := farm.Fingerprint64(key)
		txn.reads = append(txn.reads, fp)
	}

	seek := y.KeyWithTs(key, txn.readTs)
	vs, err := txn.db.get(seek)
	if err != nil {
		return nil, errors.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return nil, ErrKeyNotFound
	}
	if isDeletedOrExpired(vs) {
		return nil, ErrKeyNotFound
	}

	item.key = key
	item.version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.db = txn.db
	item.vptr = vs.Value
	item.txn = txn
	//log.Println("txn.Get()", item.vptr)
	return item, nil
}

func (txn *Txn) Discard() {
	if txn.discarded {
		return
	}
	txn.discarded = true

	for _, cb := range txn.callbacks {
		cb()
	}
	if txn.update {
		txn.db.orc.decrRef()
	}
}

// Commit commits the transaction, following these steps:
//
// 1. If there are no writes, return immediately.
//
// 2. Check if read rows were updated since txn started. If so, return ErrConflict.
//
// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
//
// 4. Batch up all writes, write them to value log and LSM tree.
//
// 5. If callback is provided, Badger will return immediately after checking
// for conflicts. Writes to the database will happen in the background.  If
// there is a conflict, an error will be returned and the callback will not
// run. If there are no conflicts, the callback will be called in the
// background upon successful completion of writes or any error during write.
//
// If error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
// tree won't be updated, so there's no need for any rollback.
func (txn *Txn) Commit(callback func(error)) error {
	if txn.commitTs == 0 && txn.db.opt.managedTxns {
		return ErrManagedTxn
	}
	if txn.discarded {
		return ErrDiscardedTxn
	}
	defer txn.Discard()
	if len(txn.writes) == 0 {
		return nil // Nothing to write
	}

	state := txn.db.orc
	commitTs := state.newCommitTs(txn)
	if commitTs == 0 {
		return ErrConflict
	}

	entries := make([]*Entry, 0, len(txn.pendingWrites)+1)
	for _, e := range txn.pendingWrites {
		//log.Println("after commit set ", e.Value)
		// Suffix the keys with commit ts, so the key versions are sorted in
		// descending order of commit timestamp.
		e.Key = y.KeyWithTs(e.Key, commitTs)
		e.meta |= bitTxn
		entries = append(entries, e)
	}
	e := &Entry{
		Key:   y.KeyWithTs(txnKey, commitTs),
		Value: []byte(strconv.FormatUint(commitTs, 10)),
		meta:  bitFinTxn,
	}
	entries = append(entries, e) // 将新的写事务添加到待执行任务中

	if callback == nil {
		// If batchSet failed, LSM would not have been updated. So, no need to rollback anything.

		// TODO: What if some of the txns successfully make it to value log, but others fail.
		// Nothing gets updated to LSM, until a restart happens.
		defer state.doneCommit(commitTs)
		return txn.db.batchSet(entries)
	}
	return txn.db.batchSetAsync(entries, func(err error) {
		state.doneCommit(commitTs)
		callback(err)
	})
}

func (db *DB) NewTransaction(update bool) *Txn {
	txn := &Txn{
		update: update,
		db:     db,
		readTs: db.orc.readTs(),
		count:  1,                       // One extra Entry for BitFin.
		size:   int64(len(txnKey) + 10), // Some buffer for the extra Entry.
	}
	if update {
		txn.pendingWrites = make(map[string]*Entry)
		txn.db.orc.addRef()
	}
	return txn
}

// View executes a function creating and managing a read-only transaction for the user. Error
// returned by the function is relayed by the View method.
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.opt.managedTxns {
		return ErrManagedTxn
	}
	txn := db.NewTransaction(false)
	defer txn.Discard()

	return fn(txn)
}

// Update executes a function, creating and managing a read-write transaction
// for the user. Error returned by the function is relayed by the Update method.
func (db *DB) Update(fn func(txn *Txn) error) error {
	if db.opt.managedTxns {
		return ErrManagedTxn
	}
	txn := db.NewTransaction(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit(nil)
}
