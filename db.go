package lsmdb

import (
	"bytes"
	"container/heap"
	"expvar"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/impact-eintr/lsmdb/skl"
	"github.com/impact-eintr/lsmdb/table"
	"github.com/impact-eintr/lsmdb/y"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"
)

var (
	lsmdbPrefix = []byte("!wlsmdb!")     // Prefix for internal keys used by wisckey lsmdb.
	head        = []byte("!wlsmdb!head") // For storing value offset for replay.
	txnKey      = []byte("!wlsmdb!txn")  // For indicating end of entries in txn.
)

type closers struct {
	updateSize *y.Closer
	compactors *y.Closer
	memtable   *y.Closer
	writes     *y.Closer
	valueGC    *y.Closer
}

type DB struct {
	sync.RWMutex

	dirLockGuard  *directoryLockGuard
	valueDirGuard *directoryLockGuard // nil if Dir and ValueDir are the same

	closers   closers
	elog      trace.EventLog
	mt        *skl.Skiplist   // Out lastest (actively written) in-memory table.
	imm       []*skl.Skiplist // Add here only AFTER pushing to flushChan.
	opt       Options
	manifest  *manifestFile
	lc        *levelsController
	vlog      valueLog
	vptr      valuePointer // Less than or equal to a pointer to the vlog value put into mt
	writeCh   chan *request
	flushChan chan flushTask // For flushing memtables.

	orc *oracle
}

const (
	kvWriteChCapacity = 1000
)

func replayFunction(out *DB) func(entry, valuePointer) error {
	type txnEntry struct {
		nk []byte
		v  y.ValueStruct
	}

	var txn []txnEntry
	var lastCommit uint64

	toLSM := func(nk []byte, vs y.ValueStruct) {
		for err := out.ensureRoomForWrite(); err != nil; err = out.ensureRoomForWrite() {
			out.elog.Printf("Replay: Making room for writes")
			time.Sleep(10 * time.Millisecond)
		}
		out.mt.Put(nk, vs)
	}

	first := true
	return func(e entry, vp valuePointer) error {
		if first {
			out.elog.Printf("First key=%s\n", e.Key)
		}
		first = false

		if out.orc.curRead < y.ParseTs(e.Key) {
			out.orc.curRead = y.ParseTs(e.Key)
		}

		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		var nv []byte
		meta := e.meta
		if out.shouldWriteValueToLSM(e) {
			nv = make([]byte, len(e.Value))
			copy(nv, e.Value)
		} else {
			nv = make([]byte, vptrSize)
			vp.Encode(nv)
			meta = meta | bitValuePointer
		}

		v := y.ValueStruct{
			Value:    nv,
			Meta:     meta,
			UserMeta: e.UserMeta,
		}

		if e.meta&bitFinTxn > 0 {
			txnTs, err := strconv.ParseUint(string(e.Value), 10, 64)
			if err != nil {
				return errors.Wrapf(err, "Unable to parse txn fin: %q", e.Value)
			}
			y.AssertTrue(lastCommit == txnTs)
			y.AssertTrue(len(txn) > 0)
			// Got the end of txn. Now we can store them.
			for _, t := range txn {
				toLSM(t.nk, t.v)
			}
			txn = txn[:0]
			lastCommit = 0

		} else if e.meta&bitTxn == 0 {
			// This entry is from a rewrite.
			toLSM(nk, v)

			// We shouldn't get this entry in the middle of a transaction.
			y.AssertTrue(lastCommit == 0)
			y.AssertTrue(len(txn) == 0)

		} else {
			txnTs := y.ParseTs(nk)
			if lastCommit == 0 {
				lastCommit = txnTs
			}
			y.AssertTrue(lastCommit == txnTs)
			te := txnEntry{nk: nk, v: v}
			txn = append(txn, te)
		}

		return nil
	}
}

func Open(opt Options) (db *DB, err error) {
	// 1. 构造配置参数对象
	opt.maxBatchSize = (15 * opt.MaxTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)

	// 2. 打开或创建工作目录并上锁
	for _, path := range []string{opt.Dir, opt.ValueDir} {
		dirExists, err := exists(path)
		if err != nil {
			return nil, y.Wrapf(err, "Invalid Dir: %q", path)
		}
		if !dirExists {
			return nil, ErrInvalidDir
		}
	}
	absDir, err := filepath.Abs(opt.Dir)
	if err != nil {
		return nil, err
	}
	absValueDir, err := filepath.Abs(opt.ValueDir)
	if err != nil {
		return nil, err
	}

	// 申请一个目录锁 保护sst
	dirLockGuard, err := acquireDirectoryLock(opt.Dir, lockFile)
	if err != nil {
		return nil, err
	}
	defer func() {
		if dirLockGuard != nil {
			_ = dirLockGuard.release()
		}
	}()
	// 申请一个文件锁 保护vlog文件
	var valueDirLockGuard *directoryLockGuard
	if absValueDir != absDir {
		valueDirLockGuard, err = acquireDirectoryLock(opt.ValueDir, lockFile)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if valueDirLockGuard != nil {
			_ = valueDirLockGuard.release()
		}
	}()
	if !(opt.ValueLogFileSize <= 2<<30 && opt.ValueLogFileSize >= 1<<20) {
		return nil, ErrValueLogSize
	}
	// 3. 打开或创建ManifestFile文件
	manifestFile, manifest, err := openOrCreateManifestFile(opt.Dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	// 事务相关
	orc := &oracle{
		isManaged:      opt.managedTxns,
		nextCommit:     1,
		pendingCommits: make(map[uint64]struct{}),
		commits:        make(map[uint64]uint64),
	}
	heap.Init(&orc.commitMark) // 准备之后的归并排序

	// 创建DB对象
	db = &DB{
		// 创建内存表列表,用于预写日志
		imm: make([]*skl.Skiplist, 0, opt.NumMemtables), // immutable memtable
		// 创建通知刷新任务的channel
		flushChan: make(chan flushTask, opt.NumMemtables),
		// 创建写入任务的channel
		writeCh: make(chan *request, kvWriteChCapacity),
		// 配置对象
		opt: opt,
		// manifest文件对象
		manifest: manifestFile,
		elog:     trace.NewEventLog("lsmdb", "DB"),
		// 目录锁
		dirLockGuard: dirLockGuard,
		// value目录锁
		valueDirGuard: valueDirLockGuard,
		// 创建oracle对象，用于并发事务的控制
		orc: orc,
	}

	db.closers.updateSize = y.NewCloser(1)
	go db.updateSize(db.closers.updateSize) // 内有循环阻塞机制
	// 创建一个激活的内存表
	db.mt = skl.NewSkiplist(arenaSize(opt)) // mutable memtable

	// newLevelsController potentially(潜在地) loads files in directory.
	// 创建一个level管理器
	if db.lc, err = newLevelsController(db, &manifest); err != nil {
		return nil, err
	}

	// 启动日志合并过程
	db.closers.compactors = y.NewCloser(1)
	db.lc.startCompact(db.closers.compactors)

	db.closers.memtable = y.NewCloser(1)
	go db.flushMemtable(db.closers.memtable)

	// vlog初始化
	if err = db.vlog.Open(db, opt); err != nil {
		return nil, err
	}

	// 获取DB的事务最大版本号
	headKey := y.KeyWithTs(head, math.MaxUint64)
	// Need to pass with timestamp, lsm get removes the last 8 bytes and compares key
	vs, err := db.get(headKey)
	if err != nil {
		return nil, errors.Wrap(err, "Retrieving head") // retrieving 检索
	}
	db.orc.curRead = vs.Version
	var vptr valuePointer
	if len(vs.Value) > 0 {
		vptr.Decode(vs.Value)
	}

	replayCloser := y.NewCloser(1)
	go db.doWrites(replayCloser)

	if err = db.vlog.Replay(vptr, replayFunction(db)); err != nil {
		return db, err
	}

	replayCloser.SignalAndWait() // Wait for replay to be applied first.
	db.orc.nextCommit = db.orc.curRead + 1

	// Mmap writable log 打开vlog文件, 关联mmap
	lf := db.vlog.filesMap[db.vlog.maxFid]
	if err = lf.mmap(2 * db.vlog.opt.ValueLogFileSize); err != nil {
		return db, errors.Wrapf(err, "Unable to mmap RDWR log file")
	}

	// 开启负责处理写请求的工作协程
	db.writeCh = make(chan *request, kvWriteChCapacity)
	db.closers.writes = y.NewCloser(1)
	go db.doWrites(db.closers.writes)

	// 启动vlog文件的GC过程
	db.closers.valueGC = y.NewCloser(1)
	go db.vlog.waitOnGC(db.closers.valueGC)

	// 使用过的值清空(TODO GC friendly? 有必要吗)
	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil
}

func (db *DB) Close() (err error) {
	db.elog.Printf("Closing database")
	// Stop value GC first.
	db.closers.valueGC.SignalAndWait()

	// Stop writes next.
	db.closers.writes.SignalAndWait()

	// Now close the value log.
	if vlogErr := db.vlog.Close(); err == nil {
		err = errors.Wrap(vlogErr, "DB.Close")
	}

	if !db.mt.Empty() {
		db.elog.Printf("Flushing memtable")
		for {
			pushedFlushTask := func() bool {
				db.Lock()
				defer db.Unlock()
				y.AssertTrue(db.mt != nil)
				select {
				case db.flushChan <- flushTask{db.mt, db.vptr}:
					db.imm = append(db.imm, db.mt) // Flusher will attempt to remove this from s.imm.
					db.mt = nil                    // Will segfault if we try writing! NOTE 一个小技巧
					db.elog.Printf("pushed to flush chan\n")
					return true
				default:
					// If we fail to push, we need to unlock and wait for a short while.
					// The flushing operation needs to update s.imm. Otherwise, we have a deadlock.
					// TODO: Think about how to do this more cleanly, maybe without any locks.
				}
				return false
			}()
			if pushedFlushTask {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	db.flushChan <- flushTask{nil, valuePointer{}} // Tell flusher to quit.

	db.closers.memtable.Wait()
	db.elog.Printf("Memtable flushed")

	db.closers.compactors.SignalAndWait()
	db.elog.Printf("Compaction finished")

	if lcErr := db.lc.close(); err == nil {
		err = errors.Wrap(lcErr, "DB.Close")
	}
	db.elog.Printf("Waiting for closer")
	db.closers.updateSize.SignalAndWait()

	db.elog.Finish()

	if guardErr := db.dirLockGuard.release(); err == nil {
		err = errors.Wrap(guardErr, "DB.Close")
	}
	if db.valueDirGuard != nil {
		if guardErr := db.valueDirGuard.release(); err == nil {
			err = errors.Wrap(guardErr, "DB.Close")
		}
	}
	if manifestErr := db.manifest.close(); err == nil {
		err = errors.Wrap(manifestErr, "DB.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	if syncErr := syncDir(db.opt.Dir); err == nil {
		err = errors.Wrap(syncErr, "DB.Close")
	}
	if syncErr := syncDir(db.opt.ValueDir); err == nil {
		err = errors.Wrap(syncErr, "DB.Close")
	}

	return err

}

const (
	lockFile = "LOCK"
)

func syncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

// getMemtables returns the current memtables and get references.
func (db *DB) getMemTables() ([]*skl.Skiplist, func()) {
	db.RLock()
	defer db.RUnlock()

	tables := make([]*skl.Skiplist, len(db.imm)+1)

	// Get mutable(易变的) memtable.
	tables[0] = db.mt
	tables[0].IncrRef()

	// Get immutable memtables.
	last := len(db.imm) - 1
	for i := range db.imm {
		tables[i+1] = db.imm[last-i]
		tables[i+1].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

func (db *DB) get(key []byte) (y.ValueStruct, error) {
	tables, decr := db.getMemTables() // Lock should be released.
	defer decr()                      // 用完以后 清除引用

	y.NumGets.Add(1)
	// 先遍历内存数组
	//log.Println("db.get(), lenght of tables : ", len(tables))
	//log.Printf("db.get(), tables[0] : %#v\n", tables[0])
	for i := 0; i < len(tables); i++ {
		vs := tables[i].Get(key)
		y.NumMemtableGets.Add(1)
		if vs.Meta != 0 || vs.Value != nil {
			return vs, nil
		}
	}
	return db.lc.get(key) // 再从sst中查找
}

// 更新DB对象中的最新value
func (db *DB) updateOffset(ptrs []valuePointer) {
	var ptr valuePointer
	for i := len(ptrs) - 1; i >= 0; i-- {
		p := ptrs[i]
		if !p.IsZero() {
			ptr = p
			break
		}
	}
	if ptr.IsZero() {
		return
	}
	db.Lock()
	defer db.Unlock()
	y.AssertTrue(!ptr.Less(db.vptr))
	db.vptr = ptr
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (db *DB) shouldWriteValueToLSM(e entry) bool {
	return len(e.Value) < db.opt.ValueThreshold
}

// 将 request 写到 memtable 中
func (db *DB) writeToLSM(b *request) error {
	if len(b.Ptrs) != len(b.Entries) {
		return errors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries {
		if entry.meta&bitFinTxn != 0 {
			continue
		}
		if db.shouldWriteValueToLSM(*entry) {
			//log.Println("writeToLSM() 写了一个值")
			db.mt.Put(entry.Key,
				y.ValueStruct{
					Value:     entry.Value,
					Meta:      entry.meta,
					UserMeta:  entry.UserMeta,
					ExpiresAt: entry.ExpiresAt,
				})
		} else {
			var offsetBuf [vptrSize]byte
			//log.Println("writeToLSM() 写了一个指针")
			db.mt.Put(entry.Key,
				y.ValueStruct{
					Value:     b.Ptrs[i].Encode(offsetBuf[:]), // 记录的是一个value的偏移
					Meta:      entry.meta | bitValuePointer,
					UserMeta:  entry.UserMeta,
					ExpiresAt: entry.ExpiresAt,
				})
		}
	}
	return nil
}

func (db *DB) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			//log.Printf("entry: %s写入日志文件结束 %v", r.Entries[0].Key, r.Err)
			r.Wg.Done()
		}
	}

	db.elog.Printf("writeRequests called. Writing to value log")

	// 将 reqs 写到 vlog 的日志文件中 用来从崩溃中恢复
	err := db.vlog.write(reqs)
	if err != nil {
		done(err)
		return err
	}

	// 向memtable写入数据
	db.elog.Printf("Writing to memtable")
	var count int
	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		for err := db.ensureRoomForWrite(); err != nil; err = db.ensureRoomForWrite() {
			db.elog.Printf("Making room for writes")
			// We need to poll a bit because both hasRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		if err := db.writeToLSM(b); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		db.updateOffset(b.Ptrs)
	}
	done(nil)
	db.elog.Printf("%d entries written", count)
	return nil
}

func (db *DB) sendToWriteCh(entries []*entry) (*request, error) {
	var count, size int64
	for _, e := range entries {
		size += int64(db.opt.estimateSize(e))
		count++
	}
	if count >= db.opt.maxBatchCount || size >= db.opt.maxBatchSize {
		return nil, ErrTxnTooBig
	}

	req := requestPool.Get().(*request)
	req.Entries = entries
	req.Wg = sync.WaitGroup{}
	req.Wg.Add(1)
	db.writeCh <- req
	y.NumPuts.Add(int64(len(entries)))

	return req, nil
}

func (db *DB) doWrites(lc *y.Closer) {
	defer lc.Done()
	pendingCh := make(chan struct{}, 1)

	writeRequests := func(reqs []*request) {
		if err := db.writeRequests(reqs); err != nil {
			log.Printf("ERROR in lsmdb::writeRequests: %v", err)
		}
		<-pendingCh
	}

	reqLen := new(expvar.Int)
	y.PendingWrites.Set(db.opt.Dir, reqLen)

	reqs := make([]*request, 0, 10)
	for {
		var r *request
		select {
		case r = <-db.writeCh:
		case <-lc.HasBeenClosed():
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*kvWriteChCapacity {
				pendingCh <- struct{}{} // blocking
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-db.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-lc.HasBeenClosed():
				goto closedCase
			}

		}

	closedCase:
		close(db.writeCh)
		for r := range db.writeCh { // Flush the channel
			reqs = append(reqs, r)
		}

		pendingCh <- struct{}{}
		writeRequests(reqs)
		return

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
		reqLen.Set(0)
	}
}

func (db *DB) batchSet(entries []*entry) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	req.Wg.Wait()
	req.Entries = nil
	err = req.Err
	requestPool.Put(req)
	return err
}

func (db *DB) batchSetAsync(entries []*entry, f func(error)) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	go func() {
		req.Wg.Wait()
		err := req.Err
		req.Entries = nil
		requestPool.Put(req)
		// Write is complete. Let's call the callback function now.
		f(err)
	}()
	return nil
}

var errNoRoom = errors.New("No room for write")

func (db *DB) ensureRoomForWrite() error {
	var err error
	db.Lock()
	defer db.Unlock()
	if db.mt.MemSize() < db.opt.MaxTableSize {
		return nil
	}

	// 当前 mmt 的大小已经超过限制 需要创建新的 mmt 并将现在的mmt设置为只读的 immt
	y.AssertTrue(db.mt != nil) // A nil mt indicates that DB is being closed.
	select {
	case db.flushChan <- flushTask{db.mt, db.vptr}:
		err = db.vlog.sync()
		if err != nil {
			return err
		}
		// 将当前的 memtable 设置为 immt 并创建新的 mmt
		db.elog.Printf("Flushing memtable, mt.size=%d size of flushChan: %d\n", db.mt.MemSize(), len(db.flushChan))
		// We manage to push this task. Let's modify imm.
		db.imm = append(db.imm, db.mt)
		db.mt = skl.NewSkiplist(arenaSize(db.opt))
		// New memtable is empty. We certainly have room.
		return nil
	default:
		return errNoRoom
	}
}

func arenaSize(opt Options) int64 {
	return opt.MaxTableSize + opt.maxBatchSize + opt.maxBatchCount*int64(skl.MaxNodeSize)
}

func writeLevel0Table(s *skl.Skiplist, f *os.File) error {
	iter := s.NewIterator()
	defer iter.Close()

	b := table.NewTableBuilder()
	defer b.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if err := b.Add(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	_, err := f.Write(b.Finish()) // 向Builder中添加了key and value 后 Finish() 会将他们添加到 Bloom Filter
	return err
}

type flushTask struct {
	mt   *skl.Skiplist
	vptr valuePointer
}

// 刷新Memtable的任务 将 DB.imm 刷到 sst 中
func (db *DB) flushMemtable(lc *y.Closer) error {
	defer lc.Done()

	for ft := range db.flushChan {
		if ft.mt == nil {
			return nil
		}

		if !ft.mt.Empty() {
			// Store badger head even if vptr is zero, need it for readTs
			db.elog.Printf("Storing offset: %+v\n", ft.vptr)
			offset := make([]byte, vptrSize)
			ft.vptr.Encode(offset)

			// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
			// commits.
			headTs := y.KeyWithTs(head, db.orc.commitTs())
			ft.mt.Put(headTs, y.ValueStruct{Value: offset})
		}

		fileID := db.lc.reserveFileID()
		fd, err := y.CreateSyncedFile(table.NewFilename(fileID, db.opt.Dir), true)
		if err != nil {
			return y.Wrap(err)
		}

		dirSyncCh := make(chan error)
		go func() {
			dirSyncCh <- syncDir(db.opt.Dir)
		}()

		err = writeLevel0Table(ft.mt, fd)
		dirSyncErr := <-dirSyncCh // 这里阻塞

		if err != nil {
			db.elog.Errorf("ERROR while writing to level 0: %v", err)
			return err
		}
		if dirSyncErr != nil {
			db.elog.Errorf("ERROR while syncing level directory: %v", dirSyncErr)
			return err
		}

		tbl, err := table.OpenTable(fd, db.opt.TableLoadingMode) // tbl 是一个 sst 对象
		//log.Printf("small :%s big:%s\n", tbl.Smallest(), tbl.Biggest())
		if err != nil {
			db.elog.Printf("ERROR while opening table: %v", err)
			return err
		}
		// We own a ref on tbl.
		err = db.lc.addLevel0Table(tbl) // This will incrRef (if we don't error, sure)
		tbl.DecrRef()                   // Releases our ref.
		if err != nil {
			return err
		}

		// Update s.imm. Need a lock.
		db.Lock()
		y.AssertTrue(ft.mt == db.imm[0]) //For now, single threaded.
		db.imm = db.imm[1:]
		ft.mt.DecrRef() // Return memory.
		db.Unlock()
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (db *DB) updateSize(lc *y.Closer) {
	defer lc.Done()

	metricsTicker := time.NewTicker(5 * time.Minute)
	defer metricsTicker.Stop()

	newInt := func(val int64) *expvar.Int {
		v := new(expvar.Int)
		v.Add(val)
		return v
	}

	totalSize := func(dir string) (int64, int64) {
		var lsmSize, vlogSize int64
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			ext := filepath.Ext(path)
			if ext == ".sst" {
				lsmSize += info.Size()
			} else if ext == ".vlog" {
				vlogSize += info.Size()
			}
			return nil
		})
		if err != nil {
			db.elog.Printf("Got error while calculating total size of directory: %s", dir)
		}
		return lsmSize, vlogSize
	}

	for {
		select {
		case <-metricsTicker.C:
			lsmSize, vlogSize := totalSize(db.opt.Dir)
			y.LSMSize.Set(db.opt.Dir, newInt(lsmSize))
			// If valueDir is different from dir, we'd have to do another walk.
			if db.opt.ValueDir != db.opt.Dir {
				_, vlogSize = totalSize(db.opt.ValueDir)
			}
			y.VlogSize.Set(db.opt.Dir, newInt(vlogSize))
		case <-lc.HasBeenClosed():
			return
		}
	}

}

// PurgeVersionsBelow will delete all versions of a key below the specified version
// purge 清理 净化
func (db *DB) PurgeVersionsBelow(key []byte, ts uint64) error {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	return db.purgeVersionsBelow(txn, key, ts)
}

func (db *DB) purgeVersionsBelow(txn *Txn, key []byte, ts uint64) error {
	opts := DefaultIteratorOptions
	opts.AllVersions = true
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)

	var entries []*entry

	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		item := it.Item()
		if !bytes.Equal(key, item.Key()) || item.Version() >= ts {
			continue
		}

		// Found an older version. Mark for deletion
		entries = append(entries,
			&entry{
				Key:  y.KeyWithTs(key, item.version),
				meta: bitDelete,
			})
		db.vlog.updateGCStats(item)
	}
	return db.batchSet(entries)
}

// PurgeOlderVersions deletes older versions of all keys.
func (db *DB) PurgeOlderVersions() error {
	return db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)

		var entries []*entry
		var lastKey []byte
		var count int
		var wg sync.WaitGroup
		errChan := make(chan error, 1)

		// func to check for pending error before sending off a batch for writing
		batchSetAsyncIfNoErr := func(entries []*entry) error {
			select {
			case err := <-errChan:
				return err
			default:
				wg.Add(1)
				return txn.db.batchSetAsync(entries, func(err error) {
					defer wg.Done()
					if err != nil {
						select {
						case errChan <- err:
						default:
						}
					}
				})
			}
		}

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if !bytes.Equal(lastKey, item.Key()) {
				lastKey = y.Safecopy(lastKey, item.Key())
				continue
			}
			// Found an older version. Mark for deletion
			entries = append(entries,
				&entry{
					Key:  y.KeyWithTs(lastKey, item.version),
					meta: bitDelete,
				})
			db.vlog.updateGCStats(item)
			count++

			// Batch up 1000 entries at a time and write
			if count == 1000 {
				if err := batchSetAsyncIfNoErr(entries); err != nil {
					return err
				}
				count = 0
				entries = []*entry{}
			}
		}

		// Write last batch pending deletes
		if count > 0 {
			if err := batchSetAsyncIfNoErr(entries); err != nil {
				return err
			}
		}

		wg.Wait()

		select {
		case err := <-errChan:
			return err
		default:
			return nil
		}
	})
}

func (db *DB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return ErrInvalidRequest
	}

	// Find head on disk
	headKey := y.KeyWithTs(head, math.MaxUint64)
	val, err := db.lc.get(headKey)
	if err != nil {
		return errors.Wrap(err, "Retrieving head from on-disk LSM")
	}

	var head valuePointer
	if len(val.Value) > 0 {
		head.Decode(val.Value)
	}

	// Pick a log file and run GC
	return db.vlog.runGC(discardRatio, head)
}
