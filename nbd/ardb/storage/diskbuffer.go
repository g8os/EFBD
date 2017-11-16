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
)

type DiskBuffer struct {
	cache   gcache.Cache
	evict   BufferEvict
	maxSize int
	cancel  context.CancelFunc
}

type BufferEvict func(hash zerodisk.Hash, content []byte)

//NewDiskBuffer create a new buffer cache
func NewDiskBuffer(evict BufferEvict, expiration, cleanup int64, maxSize int) *DiskBuffer {
	if expiration == 0 {
		expiration = cacheDefaultExpiration
	}

	if cleanup == 0 {
		cleanup = int64(math.Min(float64(expiration), cacheDefaultCleanup))
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &DiskBuffer{
		evict:   evict,
		maxSize: maxSize,
		cancel:  cancel,
	}

	if maxSize == 0 {
		return c
	}

	c.cache = gcache.New(maxSize).LRU().
		Expiration(time.Duration(expiration) * time.Second).
		EvictedFunc(c.onEvict).
		PurgeVisitorFunc(c.onEvict).
		Build()

	go c.flusher(ctx, time.Duration(cleanup)*time.Second)

	return c
}

func (c *DiskBuffer) flusher(ctx context.Context, d time.Duration) {
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

func (c *DiskBuffer) Flush() {
	if c.cache != nil {
		c.cache.Purge()
	}
}

func (c *DiskBuffer) onEvict(key, value interface{}) {
	if c.evict == nil {
		return
	}
	c.evict(zerodisk.Hash(key.(string)), value.([]byte))
}

func (c *DiskBuffer) Set(hash zerodisk.Hash, content []byte) {
	if c.cache == nil {
		c.evict(hash, content)
	} else {
		c.cache.Set(string(hash), content)
	}
}

func (c *DiskBuffer) Get(hash zerodisk.Hash) ([]byte, bool) {
	if c.cache == nil {
		return nil, false
	}

	data, err := c.cache.Get(string(hash))
	if err != nil {
		return nil, false
	}

	return data.([]byte), true
}

func (c *DiskBuffer) Count() int {
	return c.cache.Len()
}

func (c *DiskBuffer) Close() {
	c.cancel()
	c.Flush()
}
