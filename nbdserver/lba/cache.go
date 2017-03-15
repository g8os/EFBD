package lba

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
)

// called when an item gets evicted,
// shard can be nil, in case the shard was evicted explicitely by the user
type evictCallback func(shardIndex int64, shard *shard)

// cacheEntry defines the entry for a cache
type cacheEntry struct {
	shardIndex int64
	shard      *shard
}

// create a new shard cache
func newShardCache(sizeLimitInBytes int64, cb evictCallback) (cache *shardCache, err error) {
	if sizeLimitInBytes < BytesPerShard {
		err = fmt.Errorf("shardCache requires at least %d bytes, the size of 1 shard", BytesPerShard)
		return
	}

	// the amount of most recent shards we keep in memory
	size := sizeLimitInBytes / BytesPerShard

	cache = &shardCache{
		shards:    make(map[int64]*list.Element, size),
		evictList: list.New(),
		size:      int(size),
		onEvict:   cb,
	}
	return
}

// shardache contains all cached shards
type shardCache struct {
	shards    map[int64]*list.Element
	onEvict   evictCallback
	evictList *list.List
	size      int
}

// Add a shard linked to an index,
// creating either a new entry, or updating an existing one.
// Returns true if an old entry has been evicted.
func (c *shardCache) Add(shardIndex int64, shard *shard) bool {
	// Check for existing item
	if entry, ok := c.shards[shardIndex]; ok {
		c.evictList.MoveToFront(entry)
		entry.Value.(*cacheEntry).shard = shard
		return false
	}

	// Add new item
	entry := c.evictList.PushFront(&cacheEntry{
		shardIndex: shardIndex,
		shard:      shard,
	})
	c.shards[shardIndex] = entry

	evict := c.evictList.Len() > c.size
	// Verify size not exceeded
	if evict {
		c.removeOldest()
	}
	return evict
}

// Get a cached shard, if possible
func (c *shardCache) Get(shardIndex int64) (shard *shard, ok bool) {
	var elem *list.Element
	if elem, ok = c.shards[shardIndex]; ok {
		c.evictList.MoveToFront(elem)
		shard = elem.Value.(*cacheEntry).shard
	}

	return
}

// Delete (AKA explicitly evict) a shard from the cache
func (c *shardCache) Delete(shardIndex int64) (deleted bool) {
	var elem *list.Element
	if elem, deleted = c.shards[shardIndex]; deleted {
		// set shard to nil,
		// such that the onEvict cb will receive a nil element
		elem.Value.(*cacheEntry).shard = nil
		c.removeElement(elem)
	}

	return
}

// Serialize the entire cache
func (c *shardCache) Serialize(serialize func(int64, []byte) error) (err error) {
	if serialize == nil {
		err = errors.New("no serialization callback given")
		return
	}

	var buffer bytes.Buffer
	var entry *cacheEntry

	for elem := c.evictList.Front(); elem != nil; elem = elem.Next() {
		entry = elem.Value.(*cacheEntry)

		if !entry.shard.Dirty() {
			continue // only write dirty shards
		}

		if err = entry.shard.Write(&buffer); err != nil {
			return
		}

		if err = serialize(entry.shardIndex, buffer.Bytes()); err != nil {
			return
		}

		buffer.Reset()
	}

	return
}

// Clear the entire cache, optionally evicing the items first
func (c *shardCache) Clear(evict bool) {
	if evict && c.onEvict != nil {
		var shard *shard
		for index, elem := range c.shards {
			shard = elem.Value.(*cacheEntry).shard
			c.onEvict(index, shard)
		}
	}

	c.shards = make(map[int64]*list.Element, c.size)
	c.evictList.Init()
}

// removeOldest removes the oldest item from the cache.
func (c *shardCache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *shardCache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*cacheEntry)
	delete(c.shards, entry.shardIndex)
	if c.onEvict != nil {
		c.onEvict(entry.shardIndex, entry.shard)
	}
}
