package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
	"testing"
	"time"
)

func TestCacheSizeLimit(t *testing.T) {
	var flushed int
	evict := func(h zerodisk.Hash, block []byte) {
		flushed++
	}

	cache := NewCache(evict, 0, 0, 5)

	for i := 0; i < 10; i++ {
		key := fmt.Sprint(i)
		cache.Set(zerodisk.Hash(key), nil)
	}

	if ok := assert.Equal(t, 5, cache.Count()); !ok {
		t.Fatal()
	}

	if ok := assert.Equal(t, 5, flushed); !ok {
		t.Fatal()
	}
}

func TestCacheTimeLimit(t *testing.T) {
	key := zerodisk.Hash("test-key")

	cache := NewCache(nil, 2*time.Second, 0, 5)

	cache.Set(key, nil)

	<-time.After(3 * time.Second)

	if ok := assert.Equal(t, 0, cache.Count()); !ok {
		t.Fatal()
	}
}

func TestCacheGet(t *testing.T) {
	cache := NewCache(nil, 0, 0, 0)

	key := zerodisk.Hash("test-key")
	data := []byte("hello world")
	cache.Set(key, data)

	value, ok := cache.Get(key)
	if !ok {
		t.Fatal("key not found")
	}

	if ok := assert.Equal(t, data, value); !ok {
		t.Fatal()
	}
}
