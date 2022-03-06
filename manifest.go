package lsmdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/impact-eintr/lsmdb/protos"
	"github.com/impact-eintr/lsmdb/y"
	"github.com/pkg/errors"
)

// 载货单，货单；旅客名单
// Manifest represnts the contents of the MANIFEST file in a Badger store.
// The MANIFEST file describes the startup state of the db -- all LSM files and what level they're at.
// It consists of(包括) a sequence of(一系列的) ManifestChangeSet objects.
// Each of these is treated atomically,
// and contains a sequence of ManifestChange's (file creations/deletions) which we use to
type Manifest struct {
	Levels []levelManifest          // L1~Lx
	Tables map[uint64]tableManifest // L0

	// Contains(包含) total number of creation and deletion changes in the manifest -- used to compute
	Creations int // 创造物
	Deletions int // 删除部分
}

func createManifest() Manifest {
	return Manifest{
		Levels: make([]levelManifest, 0),
		Tables: make(map[uint64]tableManifest),
	}
}

// levelManifest contains information about LSM tree levels
type levelManifest struct {
	Tables map[uint64]struct{} // L1~Lx Set of table id's
}

// tableManifest contains information about a specific(特定的) level
type tableManifest struct {
	Level uint8
}

// manifestFile holds the file pointer (and other info) about the manifest file, which is a log
// file we append to.
type manifestFile struct {
	fp        *os.File
	directory string
	// We make this configurable so that unit tests can hit rewrite() code quickly.
	deletionsRewriteThreshold int

	// Guards appends,which includes access to(接近) the mainfest field.
	appendLock sync.Mutex

	// Used to track the current state of the manifest, used when rewriting.
	manifest Manifest
}

const (
	// ManifestFilename is the filename for the manifest file.
	ManifestFilename                  = "MANIFEST"
	manifestRewriteFilename           = "MANIFEST-REWRITE"
	manifestDeletionsRewriteThreshold = 10000
	manifestDeletionsRatio            = 10
)

// asChanges returns a sequence of changes that could be used to recreate the Manifest in its
// present state.
func (m *Manifest) asChanges() []*protos.ManifestChange {
	changes := make([]*protos.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, makeTableCreateChange(id, int(tm.Level)))
	}
	return changes
}

func (m *Manifest) clone() Manifest {
	changeSet := protos.ManifestChangeSet{Changes: m.asChanges()}
	ret := createManifest()
	y.Check(applyChangeSet(&ret, &changeSet))
	return ret
}

func openOrCreateManifestFile(dir string) (ret *manifestFile, result Manifest, err error) {
	return helpOpenOrCreateManifestFile(dir, manifestDeletionsRewriteThreshold)
}

func helpOpenOrCreateManifestFile(dir string, deletionsThreshold int) (ret *manifestFile, result Manifest, err error) {
	path := filepath.Join(dir, ManifestFilename)
	fp, err := y.OpenExistingSyncedFile(path, false) // We explicitly(显式地) sync in addChanges, outside the lock.
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, Manifest{}, err
		}
		m := createManifest()
		fp, netCreations, err := helpRewrite(dir, &m)
		if err != nil {
			return nil, Manifest{}, err
		}
		y.AssertTrue(netCreations == 0)
		mf := &manifestFile{
			fp:                        fp,
			directory:                 dir,
			manifest:                  m.clone(),
			deletionsRewriteThreshold: deletionsThreshold,
		}
		return mf, m, nil
	}

	// Replay manifest from the manifestfile
	manifest, truncOffset, err := ReplayManifestFile(fp)
	if err != nil {
		_ = fp.Close()
		return nil, Manifest{}, err
	}

	// Truncate file so we don't have a half-written entry at the end.
	if err := fp.Truncate(truncOffset); err != nil {
		_ = fp.Close()
		return nil, Manifest{}, err
	}

	// 之前已经将文件截断到有效数据的末尾 将文件读指针偏移到文件尾
	if _, err = fp.Seek(0, io.SeekEnd); err != nil {
		_ = fp.Close()
		return nil, Manifest{}, err
	}

	mf := &manifestFile{
		fp:                        fp,
		directory:                 dir,
		manifest:                  manifest.clone(),
		deletionsRewriteThreshold: deletionsThreshold,
	}
	return mf, manifest, nil
}

func (mf *manifestFile) close() error {
	return mf.fp.Close()
}

func (mf *manifestFile) addChanges(changesParam []*protos.ManifestChange) error {
	changes := protos.ManifestChangeSet{Changes: changesParam}
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}

	// Maybe we could use O_APPEND instead (on certain file systems)
	mf.appendLock.Lock()
	if err := applyChangeSet(&mf.manifest, &changes); err != nil {
		mf.appendLock.Unlock()
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if mf.manifest.Deletions > mf.deletionsRewriteThreshold &&
		mf.manifest.Deletions > manifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			mf.appendLock.Unlock()
			return err
		}
	} else {
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, y.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.fp.Write(buf); err != nil {
			mf.appendLock.Unlock()
			return err
		}
	}
	mf.appendLock.Unlock()

	return mf.fp.Sync()
}

// Has to be 4 bytes.  The value can never change, ever, anyway.
var magicText = [4]byte{'W', 'L', 'S', 'M'} // wisckey lsm tree

// The magic version number.
const magicVersion = 4

func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, manifestRewriteFilename)
	// We explicitly(显式地) sync.
	fp, err := y.OpenTruncFile(rewritePath, false)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)
	copy(buf[0:4], magicText[:])
	binary.BigEndian.PutUint32(buf[4:8], magicVersion)

	netCreations := len(m.Tables)
	changes := m.asChanges()
	set := protos.ManifestChangeSet{Changes: changes}

	changeBuf, err := set.Marshal()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, y.CastagnoliCrcTable))
	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, changeBuf...)
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}

	// In Windows the files should be closed before doing a Rename.
	if err = fp.Close(); err != nil {
		return nil, 0, err
	}
	manifestPath := filepath.Join(dir, ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, 0, err
	}
	fp, err = y.OpenExistingSyncedFile(manifestPath, false)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := syncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, netCreations, nil
}

func (mf *manifestFile) rewrite() error {
	// In Windows the files should be closed before doing a Rename.
	if err := mf.fp.Close(); err != nil {
		return err
	}
	fp, netCreations, err := helpRewrite(mf.directory, &mf.manifest)
	if err != nil {
		return err
	}
	mf.fp = fp
	mf.manifest.Creations = netCreations
	mf.manifest.Deletions = 0

	return nil
}

// countingReader implement the io.Reader
type countingReader struct {
	wrapped *bufio.Reader
	count   int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.wrapped.Read(p)
	r.count += int64(n)
	return
}

func (r *countingReader) ReadByte() (b byte, err error) {
	b, err = r.wrapped.ReadByte()
	if err == nil {
		r.count++
	}
	return
}

var (
	errBadMagic = errors.New("manifest has bad magic")
)

func ReplayManifestFile(fp *os.File) (ret Manifest, truncOffset int64, err error) {
	r := countingReader{wrapped: bufio.NewReader(fp)}

	var magicBuf [8]byte
	if _, err := io.ReadFull(&r, magicBuf[:]); err != nil {
		return Manifest{}, 0, errBadMagic
	}
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != magicVersion {
		return Manifest{}, 0,
			fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, magicVersion)
	}

	build := createManifest()
	var offset int64
	for {
		offset = r.count // when we calls the ReadFull() r.Read() will be called r.count will increase
		var lenCrcBuf [8]byte
		_, err := io.ReadFull(&r, lenCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return Manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCrcBuf[0:4])
		var buf = make([]byte, length)
		if _, err := io.ReadFull(&r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return Manifest{}, 0, err
		}
		if crc32.Checksum(buf, y.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			break
		}

		var changeSet protos.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return Manifest{}, 0, err
		}

		if err := applyChangeSet(&build, &changeSet); err != nil {
			return Manifest{}, 0, err
		}
	}
	return build, offset, err
}

// 在载货单里标注 添加/移除
func applyManifestChange(build *Manifest, tc *protos.ManifestChange) error {
	switch tc.Op {
	case protos.ManifestChange_CREATE:
		if _, ok := build.Tables[tc.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", tc.Id)
		}
		build.Tables[tc.Id] = tableManifest{
			Level: uint8(tc.Level),
		}
		for len(build.Levels) <= int(tc.Level) {
			build.Levels = append(build.Levels, levelManifest{make(map[uint64]struct{})})
		}
		build.Levels[tc.Level].Tables[tc.Id] = struct{}{}
		build.Creations++
	case protos.ManifestChange_DELETE:
		tm, ok := build.Tables[tc.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.Id)
		}
		delete(build.Levels[tm.Level].Tables, tc.Id)
		delete(build.Tables, tc.Id)
		build.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

// 集中处理一批载货单修改
func applyChangeSet(build *Manifest, changeSet *protos.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(build, change); err != nil {
			return err
		}
	}
	return nil
}

func makeTableCreateChange(id uint64, level int) *protos.ManifestChange {
	return &protos.ManifestChange{
		Id:    id,
		Op:    protos.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

func makeTableDeleteChange(id uint64) *protos.ManifestChange {
	return &protos.ManifestChange{
		Id: id,
		Op: protos.ManifestChange_DELETE,
	}
}
