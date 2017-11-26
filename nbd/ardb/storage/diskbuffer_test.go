package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
	"testing"
	"time"
)

func TestDiskBufferSizeLimit(t *testing.T) {
	var flushed int
	evict := func(h zerodisk.Hash, block []byte) {
		flushed++
	}

	cache := NewDiskBuffer(evict, 0, 0, 5)

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

func TestDiskBufferTimeLimit(t *testing.T) {
	key := zerodisk.Hash("test-key")

	cache := NewDiskBuffer(nil, 2, 0, 5)

	cache.Set(key, nil)

	<-time.After(3 * time.Second)

	if ok := assert.Equal(t, 0, cache.Count()); !ok {
		t.Fatal()
	}
}

func TestDiskBufferGet(t *testing.T) {
	cache := NewDiskBuffer(nil, 0, 0, 5)

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

func TestDiskBufferZeroSize(t *testing.T) {
	count := 0
	cb := func(h zerodisk.Hash, c []byte) {
		count++
	}

	cache := NewDiskBuffer(cb, 0, 0, 0)
	for i := 0; i < 10; i++ {
		cache.Set(zerodisk.Hash("test"), nil)
	}

	if ok := assert.Equal(t, 10, count); !ok {
		t.Fatal()
	}
}
