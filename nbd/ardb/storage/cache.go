package storage

import (
	"github.com/patrickmn/go-cache"
	"github.com/zero-os/0-Disk"
	"time"
)

const (
	cacheDefaultExpiration = 30 * time.Second
	cacheCleanupInterval   = 10 * time.Second
	cacheMaxSize           = 100 //each chunk is 512K so this is around 2.5M of memory
)

type Cache struct {
	cache   *cache.Cache
	evict   CacheEvict
	maxSize int
}

type CacheEvict func(hash zerodisk.Hash, content []byte)

func NewCache(evict CacheEvict, expiration, cleanupInterval time.Duration, maxSize int) *Cache {
	if expiration == time.Duration(0) {
		expiration = cacheDefaultExpiration
	}

	if cleanupInterval == time.Duration(0) {
		cleanupInterval = cacheCleanupInterval
	}

	if maxSize == 0 {
		maxSize = cacheMaxSize
	}

	c := &Cache{
		cache:   cache.New(expiration, cleanupInterval),
		evict:   evict,
		maxSize: maxSize,
	}

	c.cache.OnEvicted(c.onEvict)
	return c
}

func (c *Cache) Flush() {
	for key := range c.cache.Items() {
		c.cache.Delete(key)
	}
}

func (c *Cache) onEvict(key string, value interface{}) {
	if c.evict == nil {
		return
	}
	c.evict(zerodisk.Hash(key), value.([]byte))
}

func (c *Cache) Set(hash zerodisk.Hash, content []byte) {
	c.cache.Set(string(hash), content, cache.DefaultExpiration)

	if c.cache.ItemCount() >= c.maxSize {
		c.Flush()
	}
}

func (c *Cache) Get(hash zerodisk.Hash) ([]byte, bool) {
	data, exists := c.cache.Get(string(hash))
	if !exists {
		return nil, false
	}

	return data.([]byte), true
}

func (c *Cache) Count() int {
	return c.cache.ItemCount()
}
