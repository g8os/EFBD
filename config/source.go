package config

import (
	"context"
	"fmt"

	"github.com/zero-os/0-Disk/errors"
)

// NewSource creates a new Source based on the given configuration.
// Make sure to close the returned Source to avoid any leaks.
func NewSource(config SourceConfig) (SourceCloser, error) {
	if config.SourceType == FileSourceType {
		path, err := fileResource(config.Resource)
		if err != nil {
			return nil, errors.Wrap(err, "can't create source")
		}
		return FileSource(path)
	}

	endpoints, err := etcdResource(config.Resource)
	if err != nil {
		return nil, errors.Wrap(err, "can't create source")
	}
	return ETCDV3Source(endpoints)
}

// Source defines a minimalistic API used to fetch configs
// either once, or watching a channel for updates.
type Source interface {
	// Get a content value as a YAML byte slice,
	// using a given Key.
	Get(key Key) ([]byte, error)
	// Watch a content value based on its Key
	// and receive its value each time it is updated,
	// using a returned channel.
	Watch(ctx context.Context, key Key) (<-chan []byte, error)
	// MarkInvalidKey marks an invalid key,
	// in the hope that it can be fixed in the source,
	// and it becomes a valid key soon.
	// Optionally a vdiskID can be given,
	// in case the invalid state is related to
	// it being used for a specific vdisk.
	MarkInvalidKey(key Key, vdiskID string)
	// SourceConfig returns this source configuration.
	// For etcd this means a comma seperated list of its etcd cluster addresses.
	SourceConfig() interface{}
	// returns the type of this source
	Type() string
}

// SourceCloser defines a Source which
// can and has to be closed by the user.
type SourceCloser interface {
	Source

	// Close the config source.
	Close() error
}

// Key defines the type and unique ID of a key,
// which is used to fetch a config value in
type Key struct {
	ID   string
	Type KeyType
}

func (k Key) String() string {
	return fmt.Sprintf("%s(%s)", k.Type, k.ID)
}

// KeyType defines the type of a key,
// which together with the ID makes it unique.
type KeyType uint8

// All possible KeyType values.
const (
	KeyVdiskStatic KeyType = iota
	KeyVdiskNBD
	KeyVdiskTlog
	KeyClusterStorage
	KeyClusterZeroStor
	KeyClusterTlog
	KeyNBDServerVdisks
)

// KeyType string representations
const (
	KeyVdiskStaticStr     = "VdiskStatic"
	KeyVdiskNBDStr        = "VdiskNBD"
	KeyVdiskTlogStr       = "VdiskTlog"
	KeyClusterStorageStr  = "ClusterStorage"
	KeyClusterTlogStr     = "ClusterTlog"
	KeyNBDServerVdisksStr = "NBDServerVdisks"
)

func (kt KeyType) String() string {
	switch kt {
	case KeyVdiskStatic:
		return KeyVdiskStaticStr
	case KeyVdiskNBD:
		return KeyVdiskNBDStr
	case KeyVdiskTlog:
		return KeyVdiskTlogStr
	case KeyClusterStorage:
		return KeyClusterStorageStr
	case KeyClusterTlog:
		return KeyClusterTlogStr
	case KeyNBDServerVdisks:
		return KeyNBDServerVdisksStr
	default:
		return ""
	}
}

func watchContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}

	return context.Background()
}
