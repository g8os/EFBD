package config

import (
	"context"
	"fmt"
)

// NewSource creates a new Source based on the given configuration.
// Make sure to close the returned Source to avoid any leaks.
func NewSource(config SourceConfig) (SourceCloser, error) {
	if config.SourceType == FileSourceType {
		path, err := fileResource(config.Resource)
		if err != nil {
			return nil, fmt.Errorf("can't create source: %v", err)
		}
		return FileSource(path)
	}

	endpoints, err := etcdResource(config.Resource)
	if err != nil {
		return nil, fmt.Errorf("can't create source: %v", err)
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

// KeyType defines the type of a key,
// which together with the ID makes it unique.
type KeyType uint8

// All possible KeyType values.
const (
	KeyVdiskStatic KeyType = iota
	KeyVdiskNBD
	KeyVdiskTlog
	KeyClusterStorage
	KeyClusterTlog
	KeyNBDServerVdisks
)

func watchContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}

	return context.Background()
}
