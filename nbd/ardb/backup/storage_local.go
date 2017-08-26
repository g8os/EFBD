package backup

import (
	"errors"
	"io"
	"os"
	"os/user"
	"path"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
)

// LocalStorageDriver ceates a driver which allows you
// to read/write deduped blocks/map from/to the local file system.
func LocalStorageDriver(root string) (StorageDriver, error) {
	err := createLocalDirIfNotExists(root)
	if err != nil {
		return nil, err
	}

	return &localDriver{
		root: root,
		dirs: newDirCache(),
	}, nil
}

type localDriver struct {
	root string
	dirs *dirCache
}

// SetDedupedBlock implements StorageDriver.SetDedupedBlock
func (ld *localDriver) SetDedupedBlock(hash zerodisk.Hash, r io.Reader) error {
	dir, file, ok := hashAsDirAndFile(hash)
	if !ok {
		return errInvalidHash
	}

	return ld.writeFile(dir, file, r, false)
}

// SetDedupedMap implements StorageDriver.SetDedupedMap
func (ld *localDriver) SetDedupedMap(id string, r io.Reader) error {
	return ld.writeFile(backupDir, id, r, true)
}

// GetDedupedBlock implements StorageDriver.GetDedupedBlock
func (ld *localDriver) GetDedupedBlock(hash zerodisk.Hash, w io.Writer) error {
	dir, file, ok := hashAsDirAndFile(hash)
	if !ok {
		return errInvalidHash
	}

	return ld.readFile(dir, file, w)
}

// GetDedupedMap implements StorageDriver.GetDedupedMap
func (ld *localDriver) GetDedupedMap(id string, w io.Writer) error {
	return ld.readFile(backupDir, id, w)
}

// Close implements StorageDriver.Close
func (ld *localDriver) Close() error {
	return nil // nothing to do
}

func (ld *localDriver) readFile(dir, name string, w io.Writer) error {
	path := path.Join(ld.root, dir, name)

	log.Debug("reading content from: ", path)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrDataDidNotExist
		}
		return err
	}
	defer file.Close()

	_, err = io.Copy(w, file)
	return err
}

func (ld *localDriver) writeFile(dir, name string, r io.Reader, overwrite bool) error {
	dir = path.Join(ld.root, dir)
	err := ld.mkdirs(dir)
	if err != nil {
		return err
	}

	path := path.Join(dir, name)

	if exists, _ := localFileExists(path, false); exists {
		if !overwrite {
			// deduped blocks aren't supposed to be overwritten,
			// as their hash should indicate that the content is the same
			return nil
		}

		err = os.Remove(path)
		if err != nil {
			return err
		}
	}

	log.Debug("writing content to: ", path)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, r)
	return err
}

func (ld *localDriver) mkdirs(path string) error {
	if ld.dirs.CheckDir(path) {
		return nil
	}

	err := createLocalDirIfNotExists(path)
	if err != nil {
		return err
	}

	ld.dirs.AddDir(path)
	return nil
}

func createLocalDirIfNotExists(path string) error {
	exists, err := localFileExists(path, true)
	if err != nil || exists {
		return err
	}

	return os.MkdirAll(path, os.ModePerm)
}

func localFileExists(path string, dir bool) (bool, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return false, err
	}

	if !dir && stat.IsDir() {
		return false, errors.New("unexpected directory: " + path)
	}
	if dir && !stat.IsDir() {
		return false, errors.New("unexpected file: " + path)
	}

	return true, nil
}

var (
	defaultLocalRoot = func() string {
		var homedir string
		if usr, err := user.Current(); err == nil {
			homedir = usr.HomeDir
		} else {
			homedir = os.Getenv("HOME")
		}

		return path.Join(homedir, ".zero-os/nbd/vdisks")
	}()
)
