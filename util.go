package lsmdb

import (
	"io/ioutil"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/impact-eintr/lsmdb/table"
	"github.com/impact-eintr/lsmdb/y"
	"github.com/pkg/errors"
)

// summary is produced when DB is closed. Currently it is used only for testing.
type summary struct {
	fileIDs map[uint64]bool
}

func (s *levelsController) getSummary() *summary {
	out := &summary{
		fileIDs: make(map[uint64]bool),
	}
	for _, l := range s.levels {
		l.getSummary(out)
	}
	return out
}

func (s *levelHandler) getSummary(sum *summary) {
	s.RLock()
	defer s.RUnlock()
	for _, t := range s.tables {
		sum.fileIDs[t.ID()] = true
	}
}

func (s *DB) validate() error { return s.lc.validate() }

func (s *levelsController) validate() error {
	for _, l := range s.levels {
		if err := l.validate(); err != nil {
			return errors.Wrap(err, "Levels Controller")
		}
	}
	return nil
}

// Check does some sanity check on one level of data or in-memory index.
func (s *levelHandler) validate() error {
	if s.level == 0 {
		return nil
	}

	s.RLock()
	defer s.RUnlock()
	numTables := len(s.tables)
	for j := 1; j < numTables; j++ {
		if j >= len(s.tables) {
			return errors.Errorf("Level %d, j=%d numTables=%d", s.level, j, numTables)
		}

		if y.CompareKeys(s.tables[j-1].Biggest(), s.tables[j].Smallest()) >= 0 {
			return errors.Errorf(
				"Inter: %q vs %q: level=%d j=%d numTables=%d",
				string(s.tables[j-1].Biggest()), string(s.tables[j].Smallest()), s.level, j, numTables)
		}

		if y.CompareKeys(s.tables[j].Smallest(), s.tables[j].Biggest()) > 0 {
			return errors.Errorf(
				"Intra: %q vs %q: level=%d j=%d numTables=%d",
				s.tables[j].Smallest(), s.tables[j].Biggest(), s.level, j, numTables)
		}
	}
	return nil
}

// func (s *KV) debugPrintMore() { s.lc.debugPrintMore() }

// // debugPrintMore shows key ranges of each level.
// func (s *levelsController) debugPrintMore() {
// 	s.Lock()
// 	defer s.Unlock()
// 	for i := 0; i < s.kv.opt.MaxLevels; i++ {
// 		s.levels[i].debugPrintMore()
// 	}
// }

// func (s *levelHandler) debugPrintMore() {
// 	s.RLock()
// 	defer s.RUnlock()
// 	s.elog.Printf("Level %d:", s.level)
// 	for _, t := range s.tables {
// 		y.Printf(" [%s, %s]", t.Smallest(), t.Biggest())
// 	}
// 	y.Printf("\n")
// }

// reserveFileID ??????????????????????????? ID
func (s *levelsController) reserveFileID() uint64 {
	id := atomic.AddUint64(&s.nextFileID, 1)
	return id - 1
}

func getIDMap(dir string) map[uint64]struct{} {
	fileInfos, err := ioutil.ReadDir(dir)
	y.Check(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID, ok := table.ParseFileID(info.Name())
		if !ok {
			continue
		}
		idMap[fileID] = struct{}{}
	}
	return idMap
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
