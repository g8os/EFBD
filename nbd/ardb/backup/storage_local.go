package backup

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
)

// LocalStorageDriverConfig is used to configure and create a local (Storage) Driver.
type LocalStorageDriverConfig struct {
	// Path of the (local) root directory,
	// where the backup(s) will be r/w from/to.
	Path string
}

// LocalStorageDriver ceates a driver which allows you
// to read/write deduped blocks/map from/to the local file system.
func LocalStorageDriver(cfg LocalStorageDriverConfig) (StorageDriver, error) {
	err := createLocalDirIfNotExists(cfg.Path)
	if err != nil {
		return nil, err
	}

	return &localDriver{
		root: cfg.Path,
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

// SetHeader implements StorageDriver.SetHeader
func (ld *localDriver) SetHeader(id string, r io.Reader) error {
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

// GetHeader implements StorageDriver.GetHeader
func (ld *localDriver) GetHeader(id string, w io.Writer) error {
	return ld.readFile(backupDir, id, w)
}

// GetHeaders implements ServerDriver.GetHeaders
func (ld *localDriver) GetHeaders() (ids []string, err error) {
	dir := path.Join(ld.root, backupDir)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("couldn't read local backup dir (%s): %v", dir, err)
	}

	for _, fileInfo := range files {
		if !fileInfo.IsDir() {
			ids = append(ids, fileInfo.Name())
		}
	}
	return ids, nil
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
	// DefaultLocalRoot defines the default dir
	// used for local backup storage.
	DefaultLocalRoot = func() string {
		var homedir string
		if usr, err := user.Current(); err == nil {
			homedir = usr.HomeDir
		} else {
			homedir = os.Getenv("HOME")
		}

		return path.Join(homedir, ".zero-os/nbd/vdisks")
	}()
)
