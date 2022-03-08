package lsmdb

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/impact-eintr/lsmdb/protos"
	"github.com/impact-eintr/lsmdb/table"
	"github.com/impact-eintr/lsmdb/y"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"
)

// SST

type levelsController struct {
	elog trace.EventLog

	// The following are initialized once and const.
	levels []*levelHandler
	kv     *DB

	nextFileID uint64        // Atomic
	cstatus    compactStatus // 合并状态
}

var (
	// This is for getting timings between stalls(货摊)
	lastUnstalled time.Time
)

// revertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest.  idMap is a set of table file id's that were read from the directory
// listing.
// revertToManifest 检查所有必要的表文件是否存在并删除清单未引用的所有表文件。
// idMap 是一组从目录列表中读取的表文件 ID。
func revertToManifest(kv *DB, mf *Manifest, idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.Tables[id]; !ok {
			kv.elog.Printf("Table file %d not referenced in MANIFEST\n", id)
			filename := table.NewFilename(id, kv.opt.Dir)
			if err := os.Remove(filename); err != nil {
				return y.Wrapf(err, "While removing table %d", id)
			}
		}
	}

	return nil
}

func newLevelsController(kv *DB, mf *Manifest) (*levelsController, error) {
	y.AssertTrue(kv.opt.NumLevelZeroTablesStall > kv.opt.NumLevelZeroTables)
	s := &levelsController{
		kv:     kv,
		elog:   kv.elog,
		levels: make([]*levelHandler, kv.opt.MaxLevels), // 设置SST最大层数
	}
	s.cstatus.levels = make([]*levelCompactStatus, kv.opt.MaxLevels)

	for i := 0; i < kv.opt.MaxLevels; i++ {
		s.levels[i] = newLevelHandler(kv, i) // 新建当前层的处理器
		if i == 0 {
			// DO noting
		} else if i == 1 {
			// Level 1 probably shouldn't be too much bigger than level 0.
			s.levels[i].maxTotalSize = kv.opt.LevelOneSize
		} else {
			s.levels[i].maxTotalSize = s.levels[i-1].maxTotalSize * int64(kv.opt.LevelSizeMultiplier)
		}
		s.cstatus.levels[i] = new(levelCompactStatus)
	}

	// Compare manifest against directory, check for existent/non-existent files, and remove.
	if err := revertToManifest(kv, mf, getIDMap(kv.opt.Dir)); err != nil {
		return nil, err
	}

	// Some files may be deleted. Let's reload.
	// 创建一个tables的二维数组
	tables := make([][]*table.Table, kv.opt.MaxLevels)
	var maxFileID uint64
	// 下面的逻辑将所有的 sst 文件还原成 Table 对象 然后并到 levelsController 中
	for fileID, tableManifest := range mf.Tables {
		fname := table.NewFilename(fileID, kv.opt.Dir)
		fd, err := y.OpenExistingSyncedFile(fname, true)
		if err != nil {
			closeAllTables(tables)
			return nil, errors.Wrapf(err, "Opening file: %q", fname)
		}

		// 初始化每一个.sst结尾的文件关联为一个mmap文件
		t, err := table.OpenTable(fd, kv.opt.TableLoadingMode)
		if err != nil {
			closeAllTables(tables)
			return nil, errors.Wrapf(err, "Opening table: %q", fname)
		}
		// 根据manfist里的记录，初始化每一层的table对象
		level := tableManifest.Level
		tables[level] = append(tables[level], t)

		if fileID > maxFileID {
			maxFileID = fileID
		}
	}
	s.nextFileID = maxFileID + 1
	// 如果是L0层则按fid排序，否则按每个表中最小key进行排序
	for i, tbls := range tables {
		s.levels[i].initTables(tbls)
	}
	// Make sure key ranges do not overlap etc.
	if err := s.validate(); err != nil {
		_ = s.cleanupLevels()
		return nil, errors.Wrap(err, "Level validation")
	}

	// Sync directory (because we have at least removed some files, or previously created the
	// manifest file).
	if err := syncDir(kv.opt.Dir); err != nil {
		_ = s.close()
		return nil, err
	}

	return s, nil

}

// Closes the tables, for cleanup in newLevelsController.  (We Close() instead of using DecrRef()
// because that would delete the underlying files.)  We ignore errors, which is OK because tables
// are read-only.
func closeAllTables(tables [][]*table.Table) {
	for _, tableSlice := range tables {
		for _, table := range tableSlice {
			_ = table.Close()
		}
	}
}

func (s *levelsController) cleanupLevels() error {
	var firstErr error
	for _, l := range s.levels {
		if err := l.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *levelsController) startCompact(lc *y.Closer) {
	n := s.kv.opt.NumCompactors
	lc.AddRunning(n - 1)
	for i := 0; i < n; i++ {
		go s.runWorker(lc)
	}
}

func (s *levelsController) runWorker(lc *y.Closer) {
	defer lc.Done()
	if s.kv.opt.DoNotCompact {
		return
	}

	time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			prios := s.pickCompactLevels()
			for _, p := range prios {
				didCompact, _ := s.doCompact(p)
				if didCompact {
					break
				}
			}
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// Returns true if level zero may be compacted, without accounting for compactions that already
// might be happening.
func (s *levelsController) isLevel0Compactable() bool {
	return s.levels[0].numTables() >= s.kv.opt.NumLevelZeroTables
}

// Returns true if the non-zero level may be compacted.  delSize provides the size of the tables
// which are currently being compacted so that we treat them as already having started being
// compacted (because they have been, yet their size is already counted in getTotalSize).
func (l *levelHandler) isCompactable(delSize int64) bool {
	return l.getTotalSize()-delSize >= l.maxTotalSize
}

type compactionPriority struct {
	level int     // 将要合并的层号
	score float64 // 用来排序
}

// pickCompactLevel determines(判断) which level to compact.
func (s *levelsController) pickCompactLevels() (prios []compactionPriority) {
	if !s.cstatus.overlapsWith(0, infRange) && s.isLevel0Compactable() {
		pri := compactionPriority{
			level: 0,
			score: float64(s.levels[0].numTables()) / float64(s.kv.opt.NumLevelZeroTables),
		}
		prios = append(prios, pri)
	}
	for i, l := range s.levels[1:] {
		// Don't consider those tables that are already being compacted right now.
		delSize := s.cstatus.delSize(i + 1)

		if l.isCompactable(delSize) {
			pri := compactionPriority{
				level: i + 1,
				score: float64(l.getTotalSize()-delSize) / float64(l.maxTotalSize),
			}
			prios = append(prios, pri)
		}
	}
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].score > prios[j].score
	})
	return prios
}

// TODO 这个函数没有解析
func (s *levelsController) compactBuildTables(l int, cd compactDef) ([]*table.Table, func() error, error) {
	topTables := cd.top
	botTables := cd.bot

	var iters []y.Iterator
	if l == 0 {
		iters = appendIteratorsReversed(iters, topTables, false)
	} else {
		y.AssertTrue(len(topTables) == 1)
		iters = []y.Iterator{topTables[0].NewIterator(false)}
	}

	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	iters = append(iters, table.NewConcatIterator(botTables, false))
	it := y.NewMergeIterator(iters, false)
	defer it.Close() // Important to close the iterator to do ref counting.

	it.Rewind()

	// Start generating new tables.
	type newTableResult struct {
		table *table.Table
		err   error
	}
	resultCh := make(chan newTableResult)
	var i int
	for ; it.Valid(); i++ {
		timeStart := time.Now()
		builder := table.NewTableBuilder()
		for ; it.Valid(); it.Next() {
			if builder.ReachedCapacity(s.kv.opt.MaxTableSize) {
				break
			}
			y.Check(builder.Add(it.Key(), it.Value()))
		}
		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		y.AssertTrue(!builder.Empty())

		cd.elog.LazyPrintf("LOG Compact. Iteration to generate one table took: %v\n", time.Since(timeStart))

		fileID := s.reserveFileID()
		go func(builder *table.Builder) {
			defer builder.Close()

			fd, err := y.CreateSyncedFile(table.NewFilename(fileID, s.kv.opt.Dir), true)
			if err != nil {
				resultCh <- newTableResult{nil, errors.Wrapf(err, "While opening new table: %d", fileID)}
				return
			}

			if _, err := fd.Write(builder.Finish()); err != nil {
				resultCh <- newTableResult{nil, errors.Wrapf(err, "Unable to write to file: %d", fileID)}
				return
			}

			tbl, err := table.OpenTable(fd, s.kv.opt.TableLoadingMode)
			// decrRef is added below.
			resultCh <- newTableResult{tbl, errors.Wrapf(err, "Unable to open table: %q", fd.Name())}
		}(builder)
	}

	newTables := make([]*table.Table, 0, 20)

	// Wait for all table builders to finish.
	var firstErr error
	for x := 0; x < i; x++ {
		res := <-resultCh
		newTables = append(newTables, res.table)
		if firstErr == nil {
			firstErr = res.err
		}
	}

	if firstErr == nil {
		// Ensure created files' directory entries are visible.  We don't mind the extra latency
		// from not doing this ASAP after all file creation has finished because this is a
		// background operation.
		firstErr = syncDir(s.kv.opt.Dir)
	}

	if firstErr != nil {
		// An error happened.  Delete all the newly created table files (by calling DecrRef
		// -- we're the only holders of a ref).
		for j := 0; j < i; j++ {
			if newTables[j] != nil {
				newTables[j].DecrRef()
			}
		}
		errorReturn := errors.Wrapf(firstErr, "While running compaction for: %+v", cd)
		return nil, nil, errorReturn
	}

	sort.Slice(newTables, func(i, j int) bool {
		return y.CompareKeys(newTables[i].Biggest(), newTables[j].Biggest()) < 0
	})

	return newTables, func() error { return decrRefs(newTables) }, nil

}

// 构造 manifest change : 创建新的 Table 删除旧的 Table
func buildChangeSet(cd *compactDef, newTables []*table.Table) protos.ManifestChangeSet {
	changes := []*protos.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, makeTableCreateChange(table.ID(), cd.nextLevel.level))
	}
	for _, table := range cd.top {
		changes = append(changes, makeTableDeleteChange(table.ID()))
	}
	for _, table := range cd.bot {
		changes = append(changes, makeTableDeleteChange(table.ID()))
	}
	return protos.ManifestChangeSet{Changes: changes}
}

type compactDef struct {
	elog trace.Trace

	thisLevel *levelHandler
	nextLevel *levelHandler

	top []*table.Table // tables at the top
	bot []*table.Table // tables at the bottom

	thisRange keyRange
	nextRange keyRange

	thisSize int64
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

// 标记压实 Levle0
func (s *levelsController) fillTablesL0(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	cd.top = make([]*table.Table, len(cd.thisLevel.tables))
	copy(cd.top, cd.thisLevel.tables)
	if len(cd.top) == 0 {
		return false
	}
	cd.thisRange = infRange

	kr := getKeyRange(cd.top)                                                // 获取 cd.top 的 key 范围
	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, kr) // 检测 cd.top 与下一层重叠的部分
	cd.bot = make([]*table.Table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = kr
	} else {
		cd.nextRange = getKeyRange(cd.bot)
	}

	if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
		return false
	}

	return true
}

// 标记压实某层 TODO 还是没怎么看懂
func (s *levelsController) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	// cd.thisLevel == levelsController.levels[i] 是一整层的 Table
	tbls := make([]*table.Table, len(cd.thisLevel.tables))
	copy(tbls, cd.thisLevel.tables)
	if len(tbls) == 0 {
		return false
	}

	// Find the biggest table, and compact that first.
	// TODO: Try other table picking strategies.
	// 按照Table.Size()排序 大的在前
	sort.Slice(tbls, func(i, j int) bool {
		return tbls[i].Size() > tbls[j].Size()
	})

	for _, t := range tbls {
		cd.thisSize = t.Size()
		// 当前 Table 的 keyRagne
		cd.thisRange = keyRange{
			left:  t.Smallest(),
			right: t.Biggest(),
		}
		if s.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) { // 在当前层检测当前Table的重叠状态
			continue
		}
		// 没有重叠
		cd.top = []*table.Table{t}
		// 获取下一层与当前范围的重叠部分
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		cd.bot = make([]*table.Table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		// 没有重叠
		if len(cd.bot) == 0 {
			cd.bot = []*table.Table{}
			cd.nextRange = cd.thisRange
			if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot)

		if s.cstatus.overlapsWith(cd.nextLevel.level, cd.nextRange) {
			continue
		}

		if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true

	}

	return false
}

// 实际执行要是操作的函数
func (s *levelsController) runCompactDef(l int, cd compactDef) (err error) {
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	if thisLevel.level >= 0 && len(cd.bot) == 0 {
		y.AssertTrue(len(cd.top) == 1)
		tbl := cd.top[0]

		// 压实操作等同于一次更新
		// We write to the manifest _before_ we delete files (and after we created files).
		changes := []*protos.ManifestChange{
			// The order matters here -- you can't temporarily have two copies of the same
			// table id when reloading the manifest.
			makeTableDeleteChange(tbl.ID()),
			makeTableCreateChange(tbl.ID(), nextLevel.level),
		}
		if err := s.kv.manifest.addChanges(changes); err != nil {
			return err
		}

		// We have to add to nextLevel before we remove from thisLevel, not after.  This way, we
		// don't have a bug where reads would see keys missing from both levels.

		// Note: It's critical that we add tables (replace them) in nextLevel before deleting them
		// in thisLevel.  (We could finagle it atomically somehow.)  Also, when reading we must
		// read, or at least acquire s.RLock(), in increasing order by level, so that we don't skip
		// a compaction.

		if err := nextLevel.replaceTables(cd.top); err != nil {
			return err
		}
		if err := thisLevel.deleteTables(cd.top); err != nil {
			return err
		}

		cd.elog.LazyPrintf("\tLOG Compact-Move %d->%d smallest:%s biggest:%s took %v\n",
			l, l+1, string(tbl.Smallest()), string(tbl.Biggest()), time.Since(timeStart))
		return nil
	}

	newTables, decr, err := s.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables)

	// We write to the manifest _before_ we delete files (and after we created files)
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil {
		return err
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	if err := nextLevel.replaceTables(newTables); err != nil {
		return err
	}
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	cd.elog.LazyPrintf("LOG Compact %d->%d, del %d tables, add %d tables, took %v\n",
		l, l+1, len(cd.top)+len(cd.bot), len(newTables), time.Since(timeStart))
	return nil

}

func (s *levelsController) doCompact(p compactionPriority) (bool, error) {
	l := p.level                           // 将要合并的层号
	y.AssertTrue(l+1 < s.kv.opt.MaxLevels) // Sanity(神志清楚?) check.

	cd := compactDef{
		elog:      trace.New("lsmdb", "Compact"),
		thisLevel: s.levels[l],
		nextLevel: s.levels[l+1],
	}
	cd.elog.SetMaxEvents(100)
	defer cd.elog.Finish()

	cd.elog.LazyPrintf("Got compaction priority: %+v", p)

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	if l == 0 {
		// 压实 Level0
		if !s.fillTablesL0(&cd) {
			cd.elog.LazyPrintf("fillTables failed for level: %d\n", l)
			return false, nil
		}
	} else {
		// 压实 Levelx
		if !s.fillTables(&cd) {
			cd.elog.LazyPrintf("fillTables failed for level: %d\n", l)
			return false, nil
		}
	}

	cd.elog.LazyPrintf("Running for level: %d\n", cd.thisLevel.level)
	s.cstatus.toLog(cd.elog)
	if err := s.runCompactDef(l, cd); err != nil {
		// This compaction couldn't be done successfully.
		cd.elog.LazyPrintf("\tLOG Compact FAILED with error: %+v: %+v", err, cd)
		return false, err
	}

	// Done with compaction. So, remove the ranges from compaction status.
	s.cstatus.delete(cd)
	s.cstatus.toLog(cd.elog)
	cd.elog.LazyPrintf("Compaction for level: %d DONE", cd.thisLevel.level)
	return true, nil
}

func (s *levelsController) addLevel0Table(t *table.Table) error {
	err := s.kv.manifest.addChanges([]*protos.ManifestChange{
		makeTableCreateChange(t.ID(), 0),
	})
	if err != nil {
		return err
	}

	for !s.levels[0].tryAddLevel0Table(t) {
		var timeStart time.Time
		{
			s.elog.Printf("STALLED STALLED STALLED STALLED STALLED STALLED STALLED STALLED: %v\n",
				time.Since(lastUnstalled))
			s.cstatus.RLock()
			for i := 0; i < s.kv.opt.MaxLevels; i++ {
				s.elog.Printf("level=%d. Status=%s Size=%d\n",
					i, s.cstatus.levels[i].debug(), s.levels[i].getTotalSize())
			}
			s.cstatus.RUnlock()
			timeStart = time.Now()
		}

		for {
			// Passing 0 for delSize to compactable means we're treating incomplete compactions as
			// not having finished -- we wait for them to finish.  Also, it's crucial this behavior
			// replicates pickCompactLevels' behavior in computing compactability in order to
			// guarantee progress.
			if !s.isLevel0Compactable() && !s.levels[1].isCompactable(0) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		{
			s.elog.Printf("UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED: %v\n",
				time.Since(timeStart))
			lastUnstalled = time.Now()
		}
	}

	return nil
}

func (s *levelsController) close() error {
	err := s.cleanupLevels()
	return errors.Wrap(err, "levelController.Close")
}

// get returns the found value if any. If not found, we return nil.
func (s *levelsController) get(key []byte) (y.ValueStruct, error) {
	var maxVs y.ValueStruct
	for l, h := range s.levels {
		vs, err := h.get(key) // Calls h.RLock() andh.RUnlock()
		if err != nil {
			return y.ValueStruct{}, errors.Wrapf(err, "get key: %q", key)
		}
		if vs.Value == nil && vs.Meta == 0 {
			// 本层没有找到
			continue
		}
		// levele 的一定是最新版本
		if l == 0 {
			return vs, nil
		}
		// 否则一直跟踪最新版本
		if maxVs.Version < vs.Version {
			maxVs = vs
		}
	}
	return maxVs, nil
}

func appendIteratorsReversed(out []y.Iterator, th []*table.Table, reversed bool) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		out = append(out, th[i].NewIterator(reversed))
	}
	return out
}

func (s *levelsController) appendIterators(iters []y.Iterator, reversed bool) []y.Iterator {
	for _, level := range s.levels {
		iters = level.appendIterators(iters, reversed)
	}
	return iters
}
