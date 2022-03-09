package lsmdb

import (
	"fmt"
	"sort"
	"sync"

	"github.com/impact-eintr/lsmdb/table"
	"github.com/impact-eintr/lsmdb/y"
	"github.com/pkg/errors"
)

type levelHandler struct {
	// Guard tables, totalSize
	sync.RWMutex

	// For level >= 1, tables are sorted by key ranges, which do not overlap(堆叠).
	// For level 0, tables are sorted by time.
	// For level 0, newest table are at the back. Compact(合并) the oldest one first, which is at the front.
	tables    []*table.Table
	totalSize int64

	// The following are initialzed once and const
	level        int
	strLevel     string
	maxTotalSize int64
	db           *DB
}

func (s *levelHandler) getTotalSize() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalSize
}

// initTables replaces s.tables with given tables.This is done during loading
func (s *levelHandler) initTables(tables []*table.Table) {
	s.Lock()
	defer s.Unlock()

	s.tables = tables
	s.totalSize = 0
	for _, t := range tables {
		s.totalSize += t.Size()
	}
	// 如果是L0层则按fid排序，否则按每个表中最小key进行排序
	if s.level == 0 {
		// Key range will overlap. Just sort by fileID in ascending order
		// because newer tables are at the end of level 0.
		sort.Slice(s.tables, func(i, j int) bool {
			return s.tables[i].ID() < s.tables[j].ID()
		})
	} else {
		// Sort tables by keys.
		sort.Slice(s.tables, func(i, j int) bool {
			return y.CompareKeys(s.tables[i].Smallest(), s.tables[j].Smallest()) < 0
		})
	}

}

// deleteTables remove tables idx0, ..., idx1-1
func (s *levelHandler) deleteTables(toDel []*table.Table) error {
	s.Lock()

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.ID()] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables
	var newTables []*table.Table
	for _, t := range s.tables {
		_, found := toDelMap[t.ID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		s.totalSize -= t.Size()
	}
	s.tables = newTables

	s.Unlock()

	return decrRefs(toDel)
}

// 安全地替换一些 table
func (s *levelHandler) replaceTables(newTables []*table.Table) error {
	if len(newTables) == 0 {
		return nil
	}

	s.Lock()

	for _, tbl := range newTables {
		s.totalSize += tbl.Size()
		tbl.IncrRef()
	}

	kr := keyRange{
		left:  newTables[0].Smallest(),
		right: newTables[len(newTables)-1].Biggest(),
	}
	left, right := s.overlappingTables(levelHandlerRLocked{}, kr)

	toDecr := make([]*table.Table, right-left)
	for i := left; i < right; i++ {
		tbl := s.tables[i]
		s.totalSize -= tbl.Size()
		toDecr[i-left] = tbl
	}

	// To be safe, just make a copy. TODO: Be more careful and avoid copying.
	numDeleted := right - left
	numAdded := len(newTables)
	tables := make([]*table.Table, len(s.tables)-numDeleted+numAdded)
	y.AssertTrue(left == copy(tables, s.tables[:left]))
	t := tables[left:]
	y.AssertTrue(numAdded == copy(t, newTables))
	t = t[numAdded:]
	y.AssertTrue(len(s.tables[right:]) == copy(t, s.tables[right:]))
	s.tables = tables
	s.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return decrRefs(toDecr)
}

func decrRefs(tables []*table.Table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}

// 根据传入的层数 构建当前层的处理器
func newLevelHandler(db *DB, level int) *levelHandler {
	return &levelHandler{
		level:    level,
		strLevel: fmt.Sprintf("l%d", level),
		db:       db,
	}
}

func (s *levelHandler) tryAddLevel0Table(t *table.Table) bool {
	y.AssertTrue(s.level == 0)
	// Need lock as we may be deleting the first table during a level 0 compaction.
	s.Lock()
	defer s.Unlock()
	if len(s.tables) >= s.db.opt.NumLevelZeroTablesStall {
		return false
	}

	s.tables = append(s.tables, t)
	t.IncrRef()
	s.totalSize += t.Size()

	return true
}

func (s *levelHandler) numTables() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.tables)
}

func (s *levelHandler) close() error {
	s.RLock()
	defer s.RUnlock()
	var err error
	for _, t := range s.tables {
		if closeErr := t.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return errors.Wrap(err, "levelHandler.close")
}

// getTableForKey acquires a read-lock to access s.tables. It returns a list of tableHandlers.
// 返回当层的所有 sst 对象
func (s *levelHandler) getTableForKey(key []byte) ([]*table.Table, func() error) {
	s.RLock()
	defer s.RUnlock()

	if s.level == 0 {
		// For level 0, we need to check every table. Remember to make a copy as s.tables may change
		// once we exit this function, and we don't want to lock s.tables while seeking in tables.
		// CAUTION: Reverse the tables.
		out := make([]*table.Table, 0, len(s.tables))
		for i := len(s.tables) - 1; i >= 0; i-- {
			out = append(out, s.tables[i])
			s.tables[i].IncrRef()
		}
		return out, func() error {
			for _, t := range out {
				if err := t.DecrRef(); err != nil {
					return err
				}
			}
			return nil
		}
	}
	// For level >= 1, we can do a binary search as key range does not overlap.
	idx := sort.Search(len(s.tables), func(i int) bool {
		return y.CompareKeys(s.tables[i].Biggest(), key) >= 0
	})
	if idx >= len(s.tables) {
		// Given key is strictly > than every element we have.
		return nil, func() error { return nil }
	}
	tbl := s.tables[idx]
	tbl.IncrRef()
	return []*table.Table{tbl}, tbl.DecrRef
}

// get returns value for a given key or the key after that. If not found, return nil.
func (s *levelHandler) get(key []byte) (y.ValueStruct, error) {
	tables, decr := s.getTableForKey(key)
	//log.Println("在 sst level 中寻找", tables)
	keyNoTs := y.ParseKey(key)

	for _, th := range tables {
		if th.DoesNotHave(keyNoTs) {
			y.NumLSMBloomHits.Add(s.strLevel, 1)
			continue
		}

		it := th.NewIterator(false)
		defer it.Close()

		y.NumLSMGets.Add(s.strLevel, 1)
		it.Seek(key)
		if !it.Valid() {
			continue
		}
		if y.SameKey(key, it.Key()) {
			vs := it.Value()
			vs.Version = y.ParseTs(it.Key())
			return vs, decr()
		}
	}
	return y.ValueStruct{}, decr()
}

// appendIterators appends iterators to an array of iterators, for merging.
// NOTE: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelHandler) appendIterators(iters []y.Iterator, reversed bool) []y.Iterator {
	s.RLock()
	defer s.RUnlock()

	if s.level == 0 {
		// Remember to add in reverse order!
		// The newer table at the end of s.tables should be added first as it takes precedence.
		return appendIteratorsReversed(iters, s.tables, reversed)
	}
	return append(iters, table.NewConcatIterator(s.tables, reversed))
}

type levelHandlerRLocked struct{}

func (s *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	left := sort.Search(len(s.tables), func(i int) bool {
		return y.CompareKeys(kr.left, s.tables[i].Biggest()) <= 0
	})
	right := sort.Search(len(s.tables), func(i int) bool {
		return y.CompareKeys(kr.right, s.tables[i].Smallest()) < 0
	})
	return left, right
}
