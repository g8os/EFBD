package backup

import (
	"errors"
	"io"
	"sync"

	"github.com/zero-os/0-Disk"
)

var (
	// ErrInvalidConfig is the error returned when giving an invalid
	// storage (driver) config to the NewStorageDriver function.
	ErrInvalidConfig = errors.New("invalid storage driver config")

	// ErrDataDidNotExist is returned from a ServerDriver's Getter
	// method in case the requested data does not exist on the server.
	ErrDataDidNotExist = errors.New("requested data did not exist")
)

// NewStorageDriver creates a new storage driver,
// using the given Storage (Driver) Config.
func NewStorageDriver(cfg StorageConfig) (StorageDriver, error) {
	switch cfg.StorageType {
	case FTPStorageType:
		ftpServerConfig, ok := cfg.Resource.(FTPStorageConfig)
		if !ok {
			return nil, ErrInvalidConfig
		}
		return FTPStorageDriver(ftpServerConfig)

	case LocalStorageType:
		resource := cfg.Resource
		if resource == nil || resource == "" {
			return LocalStorageDriver(defaultLocalRoot)
		}

		path, ok := cfg.Resource.(string)
		if !ok {
			return nil, ErrInvalidConfig
		}
		return LocalStorageDriver(path)

	default:
		return nil, ErrInvalidConfig
	}
}

// StorageDriver defines the API of a (storage) driver,
// which allows us to read/write from/to a (backup) storage,
// the deduped blocks and map which form a backup.
type StorageDriver interface {
	SetDedupedBlock(hash zerodisk.Hash, r io.Reader) error
	SetHeader(id string, r io.Reader) error

	GetDedupedBlock(hash zerodisk.Hash, w io.Writer) error
	GetHeader(id string, w io.Writer) error

	Close() error
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
