package lba

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"sync"
)

// called when an item gets evicted,
// shard can be nil, in case the shard was evicted explicitely by the user
type evictCallback func(shardIndex int64, shard *shard)

// called for each shard that gets serialized during cache.Serialize
// bytes can be nil in case the shard only contains nil hashes
type serializeCallback func(shardIndex int64, bytes []byte) error

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

// shardCache contains all cached shards,
// only the most recent shards are kept.
type shardCache struct {
	shards    map[int64]*list.Element
	onEvict   evictCallback
	evictList *list.List
	size      int
	mux       sync.Mutex
}

// Add a shard linked to an index,
// creating either a new entry, or updating an existing one.
// Returns true if an old entry has been evicted.
func (c *shardCache) Add(shardIndex int64, shard *shard) bool {
	c.mux.Lock()
	defer c.mux.Unlock()

	// Check for existing item
	if entry, ok := c.shards[shardIndex]; ok {
		// update an existing item
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
		// size exceeded, thus evict the oldest one
		c.removeOldest()
	}
	return evict
}

// Get a cached shard, if possible,
// return false otherwise.
func (c *shardCache) Get(shardIndex int64) (shard *shard, ok bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

	var elem *list.Element
	if elem, ok = c.shards[shardIndex]; ok {
		c.evictList.MoveToFront(elem)
		shard = elem.Value.(*cacheEntry).shard
	}

	return
}

// Delete (AKA explicitly evict) a shard from the cache
func (c *shardCache) Delete(shardIndex int64) (deleted bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

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
func (c *shardCache) Serialize(onSerialize serializeCallback) (err error) {
	if onSerialize == nil {
		err = errors.New("no serialization callback given")
		return
	}

	var buffer bytes.Buffer
	var entry *cacheEntry

	c.mux.Lock()
	defer c.mux.Unlock()

	for elem := c.evictList.Front(); elem != nil; elem = elem.Next() {
		entry = elem.Value.(*cacheEntry)

		if !entry.shard.Dirty() {
			continue // only write dirty shards
		}

		if err = entry.shard.Write(&buffer); err != nil {
			if err != errNilShardWrite {
				return
			}

			// indicate that shard can be deleted,
			// as the `errNilShardWrite` indicates
			// that all hashes in this shard were nil
			err = onSerialize(entry.shardIndex, nil)
		} else {
			err = onSerialize(entry.shardIndex, buffer.Bytes())
		}

		if err != nil {
			return
		}

		buffer.Reset()
	}

	return
}

// Clear the entire cache, optionally evicing the items first
func (c *shardCache) Clear(evict bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

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
