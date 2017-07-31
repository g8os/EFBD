package config

import (
	"context"
	"errors"
)

var (
	// ErrNilSource is returned from any config API
	// function which received a nil source as its parameter.
	ErrNilSource = errors.New("no config source given")
	// ErrNilCallback is returned from any watch function which received a nill callback as its parameter.
	ErrNilCallback = errors.New("no watch callback given")
	// ErrContextDone is returned from any config watch function
	// where the context is already finished,
	// while the watch function is still being created.
	ErrContextDone = errors.New("context is already done")
)

// Source defines a minimalistic API used to fetch configs
// either once, or using a callback each time it is updated.
type Source interface {
	// Get a content value as a YAML byte slice,
	// using a given Key.
	Get(key Key) ([]byte, error)
	// Watch a content value based on its Key
	// and receive its value each time it is updated,
	// using a given callback.
	Watch(ctx context.Context, key Key, cb WatchCallback) error
	// Close the config source.
	Close() error
}

// WatchCallback is provided by a watcher of a Source.
type WatchCallback func([]byte) error

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
