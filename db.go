package lsmdb

import (
	"container/heap"
	"expvar"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/impact-eintr/lsmdb/skl"
	"github.com/impact-eintr/lsmdb/table"
	"github.com/impact-eintr/lsmdb/y"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"
)

var (
	lsmdbPrefix = []byte("!lsmdb!")     // Prefix for internal keys used by lsmdb.
	head        = []byte("!lsmdb!head") // For storing value offset for replay.
	txnKey      = []byte("!lsmdb!txn")  // For indicating end of entries in txn.
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
	mainfest  *manifestFile
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

func replyFunction(out *DB) func(entry, valuePointer) error {

}

func Open(opt Options) (db *DB, err error) {
	// 1. 构造配置参数对象
	opt.maxBatchSize = (15 * opt.MaxTableSize) / 100
	opt.maxBatchCount = opt.maxBatchCount / int64(skl.MaxNodeSize)

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
		mainfest: manifestFile,
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
	if err := db.vlog.Open(db, opt); err != nil {
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

	if err = db.vlog.Replay(vptr, replyFunction(db)); err != nil {
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

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil

}

func (db *DB) Close() (err error) {

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
	for i := 0; i < len(tables); i++ {
		vs := tables[i].Get(key)
		y.NumMemtableGets.Add(1)
		if vs.Meta != 0 || vs.Value != nil {
			return vs, nil
		}
	}
	return db.lc.get(key) // 再从sst中查找
}

func arenaSize(opt Options) int64 {
	return opt.MaxTableSize + opt.maxBatchSize + opt.maxBatchCount*int64(skl.MaxNodeSize)
}

type flushTask struct {
	mt   *skl.Skiplist
	vptr valuePointer
}

// 刷新Memtable的任务
func (db *DB) flushMemtabel(lc *y.Closer) error {
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

		}
		if dirSyncErr != nil {

		}

		tbl, err := table.OpenTable(fd, db.opt.TableLoadingMode)
		if err != nil {

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
		return false, nil
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
func (db *DB) PurgeVersionsBelow(key []byte, ts uint64) error {}

func (db *DB) purgeVersionBelow(txn *Txn, key []byte, ts uint64) error {}

// PurgeOlderVersions deletes older versions of all keys.
func (db *DB) PurgeOlderVersions() error {}

func (db *DB) RunValueLogGC(discardRatio float64) error {}
