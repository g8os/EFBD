package storage

import (
	"context"
	"github.com/bluele/gcache"
	"github.com/zero-os/0-Disk"
	"math"
	"time"
)

const (
	CacheSize              = 5 * 1024 * 1024 //CacheSize in MB
	cacheDefaultExpiration = 10
	cacheDefaultCleanup    = 5
	cacheDefaultMaxSize    = 100 //each chunk is 512K so this is around 2.5M of memory
)

type Cache struct {
	cache   gcache.Cache
	evict   CacheEvict
	maxSize int
	cancel  context.CancelFunc
}

type CacheEvict func(hash zerodisk.Hash, content []byte)

//NewCache create a new buffer cache
func NewCache(evict CacheEvict, expiration, cleanup int64, maxSize int) *Cache {
	if expiration == 0 {
		expiration = cacheDefaultExpiration
	}

	if cleanup == 0 {
		cleanup = int64(math.Max(float64(expiration), cacheDefaultCleanup))
	}

	if maxSize == 0 {
		maxSize = cacheDefaultMaxSize
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Cache{
		evict:   evict,
		maxSize: maxSize,
		cancel:  cancel,
	}

	c.cache = gcache.New(maxSize).LRU().
		Expiration(time.Duration(expiration) * time.Second).
		EvictedFunc(c.onEvict).
		PurgeVisitorFunc(c.onEvict).
		Build()

	go c.flusher(ctx, time.Duration(cleanup)*time.Second)

	return c
}

func (c *Cache) flusher(ctx context.Context, d time.Duration) {
	for {
		select {
		case <-time.After(d):
			//we need to force delete of expired objects.
			//there is no public methods to do this, but
			//requesting a list of keys will force the cache
			//to validate expiry time, removing the expired
			//objects
			c.cache.Keys()
		case <-ctx.Done():
		}
	}
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

func (c *Cache) Close() {
	c.cancel()
	c.Flush()
}
