package lba

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"sync"
)

// called when an item gets evicted,
// sector can be nil, in case the sector was evicted explicitely by the user
type evictCallback func(sectorIndex int64, sector *sector)

// called for each sector that gets serialized during cache.Serialize
// bytes can be nil in case the sector only contains nil hashes
type serializeCallback func(sectorIndex int64, bytes []byte) error

// cacheEntry defines the entry for a cache
type cacheEntry struct {
	sectorIndex int64
	sector      *sector
}

// create a new sector cache
func newSectorCache(sizeLimitInBytes int64, cb evictCallback) (cache *sectorCache, err error) {
	if sizeLimitInBytes < BytesPerSector {
		err = fmt.Errorf("sectorCache requires at least %d bytes, the size of 1 sector", BytesPerSector)
		return
	}

	// the amount of most recent sectors we keep in memory
	size := sizeLimitInBytes / BytesPerSector

	cache = &sectorCache{
		sectors:   make(map[int64]*list.Element, size),
		evictList: list.New(),
		size:      int(size),
		onEvict:   cb,
	}
	return
}

// sectorCache contains all cached sectors,
// only the most recent sectors are kept.
type sectorCache struct {
	sectors   map[int64]*list.Element
	onEvict   evictCallback
	evictList *list.List
	size      int
	mux       sync.Mutex
}

// Add a sector linked to an index,
// creating either a new entry, or updating an existing one.
// Returns true if an old entry has been evicted.
func (c *sectorCache) Add(sectorIndex int64, sector *sector) bool {
	c.mux.Lock()
	defer c.mux.Unlock()

	// Check for existing item
	if entry, ok := c.sectors[sectorIndex]; ok {
		// update an existing item
		c.evictList.MoveToFront(entry)
		entry.Value.(*cacheEntry).sector = sector
		return false
	}

	// Add new item
	entry := c.evictList.PushFront(&cacheEntry{
		sectorIndex: sectorIndex,
		sector:      sector,
	})
	c.sectors[sectorIndex] = entry

	evict := c.evictList.Len() > c.size
	// Verify size not exceeded
	if evict {
		// size exceeded, thus evict the oldest one
		c.removeOldest()
	}
	return evict
}

// Get a cached sector, if possible,
// return false otherwise.
func (c *sectorCache) Get(sectorIndex int64) (sector *sector, ok bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

	var elem *list.Element
	if elem, ok = c.sectors[sectorIndex]; ok {
		c.evictList.MoveToFront(elem)
		sector = elem.Value.(*cacheEntry).sector
	}

	return
}

// Delete (AKA explicitly evict) a sector from the cache
func (c *sectorCache) Delete(sectorIndex int64) (deleted bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

	var elem *list.Element
	if elem, deleted = c.sectors[sectorIndex]; deleted {
		// set sector to nil,
		// such that the onEvict cb will receive a nil element
		elem.Value.(*cacheEntry).sector = nil
		c.removeElement(elem)
	}

	return
}

// Serialize the entire cache
func (c *sectorCache) Serialize(onSerialize serializeCallback) (err error) {
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

		if !entry.sector.Dirty() {
			continue // only write dirty sectors
		}

		if err = entry.sector.Write(&buffer); err != nil {
			if err != errNilSectorWrite {
				return
			}

			// indicate that sector can be deleted,
			// as the `errNilSectorWrite` indicates
			// that all hashes in this sector were nil
			err = onSerialize(entry.sectorIndex, nil)
		} else {
			err = onSerialize(entry.sectorIndex, buffer.Bytes())
		}

		if err != nil {
			return
		}

		buffer.Reset()
	}

	return
}

// Clear the entire cache, optionally evicing the items first
func (c *sectorCache) Clear(evict bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if evict && c.onEvict != nil {
		var sector *sector
		for index, elem := range c.sectors {
			sector = elem.Value.(*cacheEntry).sector
			c.onEvict(index, sector)
		}
	}

	c.sectors = make(map[int64]*list.Element, c.size)
	c.evictList.Init()
}

// removeOldest removes the oldest item from the cache.
func (c *sectorCache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *sectorCache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*cacheEntry)
	delete(c.sectors, entry.sectorIndex)
	if c.onEvict != nil {
		c.onEvict(entry.sectorIndex, entry.sector)
	}
}
