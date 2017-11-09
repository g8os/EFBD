package config

import (
	"context"
	"sync"
)

// NewOnceSource creates a config source,
// that fetches the key's value from the provided source and saves in memory
// if the key is requested again, the value from cache will be returned.
// WARNING: this source is only to be used for a config that should be static,
// calling the Watch function on this source will result in a panic.
func NewOnceSource(source Source) Source {
	if source == nil {
		panic("a nil source was provided")
	}

	return &onceSource{
		source: source,
		cache:  make(map[Key][]byte),
	}
}

type onceSource struct {
	source Source
	cache  map[Key][]byte
	mux    sync.RWMutex
}

// Get implements Source.Get
func (o *onceSource) Get(key Key) ([]byte, error) {
	o.mux.RLock()
	val, ok := o.cache[key]
	o.mux.RUnlock()
	if ok {
		return val, nil
	}

	// if not in cache, get from internal source
	// and store it in the cache
	val, err := o.source.Get(key)
	if err != nil {
		return nil, err
	}
	o.mux.Lock()
	o.cache[key] = val
	o.mux.Unlock()

	return val, nil
}

// Watch implements Source.Watch
func (o *onceSource) Watch(ctx context.Context, key Key) (<-chan []byte, error) {
	panic("tried to call Watch on a static config source")
}

// MarkInvalidKey implements Source.MarkInvalidKey
func (o *onceSource) MarkInvalidKey(key Key, vdiskID string) {
	o.source.MarkInvalidKey(key, vdiskID)
}

// SourceConfig implements Source.SourceConfig
func (o *onceSource) SourceConfig() interface{} {
	return o.source.SourceConfig()
}

// Type implements Source.Type
func (o *onceSource) Type() string {
	return o.source.Type()
}
