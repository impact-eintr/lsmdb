package lsmdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

type directoryLockGuard struct {
	// File handle on the directory, which we 've flocked.
	f *os.File
	// The absolute path to our pid file.
	path string
}

func acquireDirectoryLock(dirPath string, pidFileName string) (*directoryLockGuard, error) {
	// Convert to absolute path so that Release still works even if we do an unbalanced
	// chdir in the meantime.
	absPidFilePath, err := filepath.Abs(filepath.Join(dirPath, pidFileName))
	if err != nil {
		return nil, errors.Wrap(err, "cannot get absolute path for pid lock file")
	}
	f, err := os.Open(dirPath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open directory %q", dirPath)
	}
	err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		f.Close()
		return nil, errors.Wrapf(err,
			"Cannot acquire directory lock on %q.  Another process is using this Badger database.",
			dirPath)
	}

	// Yes, we happily overwrite a pre-existing pid file.  We're the only badger process using this
	// directory.
	err = ioutil.WriteFile(absPidFilePath, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0666)
	if err != nil {
		f.Close()
		return nil, errors.Wrapf(err,
			"Cannot write pid file %q", absPidFilePath)
	}

	return &directoryLockGuard{f, absPidFilePath}, nil
}

func (guard *directoryLockGuard) release() error {
	// It's important that we remove the pid file first.
	err := os.Remove(guard.path)
	if closeErr := guard.f.Close(); err == nil {
		err = closeErr
	}
	guard.path = ""
	guard.f = nil

	return err
}

func openDir(path string) (*os.File, error) { return os.Open(path) }
