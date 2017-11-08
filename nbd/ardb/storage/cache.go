package storage

import (
	"github.com/bluele/gcache"
	"github.com/zero-os/0-Disk"
	"time"
)

const (
	CacheSize              = 5 * 1024 * 1024 //CacheSize in MB
	cacheDefaultExpiration = 30 * time.Second
	cacheDefaultMaxSize    = 100 //each chunk is 512K so this is around 2.5M of memory
)

type Cache struct {
	cache   gcache.Cache
	evict   CacheEvict
	maxSize int
}

type CacheEvict func(hash zerodisk.Hash, content []byte)

func NewCache(evict CacheEvict, expiration time.Duration, maxSize int) *Cache {
	if expiration == time.Duration(0) {
		expiration = cacheDefaultExpiration
	}

	if maxSize == 0 {
		maxSize = cacheDefaultMaxSize
	}

	c := &Cache{
		evict:   evict,
		maxSize: maxSize,
	}

	c.cache = gcache.New(maxSize).LRU().
		Expiration(expiration).
		EvictedFunc(c.onEvict).
		PurgeVisitorFunc(c.onEvict).
		Build()

	return c
}

func (c *Cache) Flush() {
	c.cache.Purge()
}

func (c *Cache) onEvict(key, value interface{}) {
	if c.evict == nil {
		return
	}
	c.evict(zerodisk.Hash(key.(string)), value.([]byte))
}

func (c *Cache) Set(hash zerodisk.Hash, content []byte) {
	c.cache.Set(string(hash), content)
}

func (c *Cache) Get(hash zerodisk.Hash) ([]byte, bool) {
	data, err := c.cache.Get(string(hash))
	if err != nil {
		return nil, false
	}

	return data.([]byte), true
}

func (c *Cache) Count() int {
	return c.cache.Len()
}
