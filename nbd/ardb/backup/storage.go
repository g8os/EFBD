package backup

import (
	"errors"
	"io"
	"sync"

	"github.com/zero-os/0-Disk"
)

var (
	// ErrDataDidNotExist is returned from a ServerDriver's Getter
	// method in case the requested data does not exist on the server.
	ErrDataDidNotExist = errors.New("requested data did not exist")
)

// StorageDriver defines the API of a (storage) driver,
// which allows us to read/write from/to a (backup) storage,
// the deduped blocks and map which form a backup.
type StorageDriver interface {
	SetDedupedBlock(hash zerodisk.Hash, r io.Reader) error
	SetHeader(id string, r io.Reader) error

	GetDedupedBlock(hash zerodisk.Hash, w io.Writer) error
	GetHeader(id string, w io.Writer) error

	GetHeaders() (ids []string, err error)

	Close() error
}

// ReadSnapshotHeader loads (read=>[decrypt=>]decompress=>decode)
// a (snapshot) header from a given (backup) storage.
//
// storagDriverConfig is used to configure the backup storage driver.
// - When not given (nil), defaults to LocalStorageDriver, using the DefaultLocalRoot as the path.
// - When given:
//  - If type equals LocalStorageDriverConfig -> Create LocalStorageDriver;
//  - If type equals FTPStorageDriverConfig -> Create FTPStorageDriver;
//  - Else -> error
func ReadSnapshotHeader(id string, storagDriverConfig interface{}, key *CryptoKey, ct CompressionType) (*Header, error) {
	// create (backup) storage driver (so we can list snapshot headers from it)
	driver, err := newStorageDriver(storagDriverConfig)
	if err != nil {
		return nil, err
	}
	return LoadHeader(id, driver, key, ct)
}

// ListSnapshots lists all snapshots on a given backup storage.
// Optionally a predicate can be given to filter the returned identifiers.
// ids can be nil, even when no error occured, this is not concidered an error.
//
// storagDriverConfig is used to configure the backup storage driver.
// - When not given (nil), defaults to LocalStorageDriver, using the DefaultLocalRoot as the path.
// - When given:
//  - If type equals LocalStorageDriverConfig -> Create LocalStorageDriver;
//  - If type equals FTPStorageDriverConfig -> Create FTPStorageDriver;
//  - Else -> error
func ListSnapshots(storagDriverConfig interface{}, pred func(id string) bool) (ids []string, err error) {
	// create (backup) storage driver (so we can list snapshot headers from it)
	driver, err := newStorageDriver(storagDriverConfig)
	if err != nil {
		return nil, err
	}

	snapshotIDs, err := driver.GetHeaders()
	if err != nil {
		return nil, err
	}
	if pred == nil {
		return snapshotIDs, nil
	}

	filterPos := 0
	var ok bool
	for _, snapshotID := range snapshotIDs {
		ok = pred(snapshotID)
		if ok {
			snapshotIDs[filterPos] = snapshotID
			filterPos++
		}
	}

	return snapshotIDs[:filterPos], nil
}

func hashAsDirAndFile(hash zerodisk.Hash) (string, string, bool) {
	if len(hash) != zerodisk.HashSize {
		return "", "", false
	}

	dir := hashBytesToString(hash[:2]) + "/" + hashBytesToString(hash[2:4])
	file := hashBytesToString(hash[4:])
	return dir, file, true
}

func hashBytesToString(bs []byte) string {
	var total []byte
	for _, b := range bs {
		total = append(total, hexadecimals[b/16], hexadecimals[b%16])
	}
	return string(total)
}

const hexadecimals = "0123456789ABCDEF"

func newDirCache() *dirCache {
	return &dirCache{knownDirs: make(map[string]struct{})}
}

type dirCache struct {
	knownDirs map[string]struct{}
	mux       sync.RWMutex
}

func (cache *dirCache) AddDir(path string) {
	cache.mux.Lock()
	defer cache.mux.Unlock()
	cache.knownDirs[path] = struct{}{}
}

func (cache *dirCache) CheckDir(path string) bool {
	cache.mux.RLock()
	defer cache.mux.RUnlock()
	_, exists := cache.knownDirs[path]
	return exists
}

// some static errors returned by a (backup) storage API
var (
	errInvalidHash = errors.New("invalid hash given")
)

const (
	backupDir = "backups"
)
