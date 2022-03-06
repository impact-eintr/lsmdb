package lsmdb

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

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

			}
		case <-lc.HasBeenClosed():
			return
		}
	}
}

type compactionPriority struct {
	level int
	score float64
}

func (s *levelsController) pickCompactLevels() (prios []compactionPriority) {
	if !s.cstatus.overlapsWith(0, infRange) && s.isLevel0Compactable() {
		pri := compactionPriority{
			level: 0,
			score: float64(s.levels[0].numTables()) / float64(s.kv.opt.NumLevelZeroTables),
		}
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

func (s *levelsController) close() error {
	err := s.cleanupLevels()
	return errors.Wrap(err, "levelController.Close")
}
