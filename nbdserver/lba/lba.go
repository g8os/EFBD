package lba

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
)

// MetaRedisProvider is used by the LBA,
// to retreive a Redis Meta Connection
type MetaRedisProvider interface {
	MetaRedisConnection() (redis.Conn, error)
}

// NewLBA creates a new LBA
func NewLBA(vdiskID string, blockCount, cacheLimitInBytes int64, provider MetaRedisProvider) (lba *LBA, err error) {
	if provider == nil {
		return nil, errors.New("NewLBA requires a non-nil MetaRedisProvider")
	}

	// create as much mutexes as we need sectors
	muxCount := blockCount / NumberOfRecordsPerLBASector
	if blockCount%NumberOfRecordsPerLBASector > 0 {
		muxCount++
	}

	lba = &LBA{
		provider:   provider,
		vdiskID:    vdiskID,
		storageKey: StorageKey(vdiskID),
		sectorMux:  make([]sync.RWMutex, muxCount),
	}

	lba.cache, err = newSectorCache(cacheLimitInBytes, lba.onCacheEviction)

	return
}

// LBA implements the functionality to lookup block keys through the logical block index.
// The data is persisted to an external metadataserver in sectors of n keys,
// where n = NumberOfRecordsPerLBASector.
type LBA struct {
	cache *sectorCache

	// One mutex per sector, allows us to only lock
	// on a per-sector basis. Even with 65k block, that's still only a ~500 element mutex array.
	// We stil need to lock on a per-sector basis,
	// as otherwise we might have a race condition where for example
	// 2 operations might create a new sector, and thus we would miss an operation.
	sectorMux []sync.RWMutex

	provider MetaRedisProvider

	vdiskID    string
	storageKey string
}

// Set the content hash for a specific block.
// When a key is updated, the sector containing this blockindex is marked as dirty and will be
// stored in the external metadataserver when Flush is called,
// or when the its getting evicted from the cache due to space limitations.
func (lba *LBA) Set(blockIndex int64, h zerodisk.Hash) (err error) {
	sectorIndex := blockIndex / NumberOfRecordsPerLBASector
	// Fetch the appropriate sector
	sector, err := func(sectorIndex int64) (sector *sector, err error) {
		lba.sectorMux[sectorIndex].Lock()
		defer lba.sectorMux[sectorIndex].Unlock()

		sector, err = lba.getSector(sectorIndex)
		if err != nil {
			return
		}
		if sector == nil {
			sector = newSector()
			// store the new sector in the cache,
			// otherwise it will be forgotten...
			lba.cache.Add(sectorIndex, sector)
		}

		return
	}(sectorIndex)

	if err != nil {
		return
	}

	//Update the hash
	hashIndex := blockIndex % NumberOfRecordsPerLBASector

	lba.sectorMux[sectorIndex].Lock()
	defer lba.sectorMux[sectorIndex].Unlock()

	sector.Set(hashIndex, h)
	return
}

// Delete the content hash for a specific block.
// When a key is updated, the sector containing this blockindex is marked as dirty and will be
// stored in the external metadaserver when Flush is called,
// or when the its getting evicted from the cache due to space limitations.
// Deleting means actually that the nilhash will be set for this blockindex.
func (lba *LBA) Delete(blockIndex int64) (err error) {
	err = lba.Set(blockIndex, nil)
	return
}

// Get returns the hash for a block, nil if no hash is registered.
// If the sector containing this blockindex is not present, it is fetched from the external metadaserver
func (lba *LBA) Get(blockIndex int64) (h zerodisk.Hash, err error) {
	sectorIndex := blockIndex / NumberOfRecordsPerLBASector

	sector, err := func(sectorIndex int64) (*sector, error) {
		lba.sectorMux[sectorIndex].RLock()
		defer lba.sectorMux[sectorIndex].RUnlock()

		return lba.getSector(sectorIndex)
	}(sectorIndex)

	if err != nil || sector == nil {
		return
	}

	// get the hash
	hashIndex := blockIndex % NumberOfRecordsPerLBASector

	lba.sectorMux[sectorIndex].RLock()
	defer lba.sectorMux[sectorIndex].RUnlock()

	h = sector.Get(hashIndex)
	if h.Equals(zerodisk.NilHash) {
		h = nil
	}

	return
}

// Flush stores all dirty sectors to the external metadaserver
func (lba *LBA) Flush() (err error) {
	err = lba.storeCacheInExternalStorage()
	return
}

// Get a sector from cache given a block index,
// or retreiving it from the external storage instead,
// in case the sector isn't available in the cache.
func (lba *LBA) getSector(index int64) (sector *sector, err error) {
	sector, ok := lba.cache.Get(index)
	if !ok {
		sector, err = lba.getSectorFromExternalStorage(index)
		if err != nil {
			return
		}

		if sector != nil {
			lba.cache.Add(index, sector)
		}
	}

	return
}

// in case a sector gets evicted from cache,
// this method will be called, and we'll serialize the sector immediately,
// unless it isn't dirty
func (lba *LBA) onCacheEviction(index int64, sector *sector) {
	if !sector.Dirty() {
		return
	}

	var err error

	// the given sector can be nil in case it was deleted by the user,
	// in that case we will remove the sector from the external storage as well
	// otherwise we serialize the sector before it gets thrown into the void
	if sector != nil {
		err = lba.storeSectorInExternalStorage(index, sector)
	} else {
		err = lba.deleteSectorFromExternalStorage(index)
	}

	if err != nil {
		log.Infof("error during eviction of sector %d: %s", index, err)
	}
}

func (lba *LBA) getSectorFromExternalStorage(index int64) (sector *sector, err error) {
	conn, err := lba.provider.MetaRedisConnection()
	if err != nil {
		return
	}
	defer conn.Close()
	reply, err := conn.Do("HGET", lba.storageKey, index)
	if err != nil || reply == nil {
		return
	}

	sectorBytes, err := redis.Bytes(reply, err)
	if err != nil {
		return
	}

	sector, err = sectorFromBytes(sectorBytes)
	return
}

// Store all sectors available in the cache,
// into the external storage.
func (lba *LBA) storeCacheInExternalStorage() (err error) {
	conn, err := lba.provider.MetaRedisConnection()
	if err != nil {
		return
	}
	defer conn.Close()

	var cmdCount int64
	lba.cache.Serialize(func(index int64, bytes []byte) (err error) {
		if bytes != nil {
			err = conn.Send("HSET", lba.storageKey, index, bytes)
		} else {
			err = conn.Send("HDEL", lba.storageKey, index)
		}

		cmdCount++
		return
	})

	// Write all sets in output buffer to Redis at once
	err = conn.Flush()
	if err != nil {
		return
	}

	// read all responses
	var errors flushError
	for i := int64(0); i < cmdCount; i++ {
		_, err = conn.Receive()
		if err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		err = errors // return 1+ errors in case we received any
		return
	}

	// no need to evict, already serialized them
	evict := false
	// clear cache, as we serialized them all
	lba.cache.Clear(evict)

	// return with no errors, all good
	return
}

// Store a single sector into the external storage.
func (lba *LBA) storeSectorInExternalStorage(index int64, sector *sector) (err error) {
	if !sector.Dirty() {
		log.Debugf(
			"LBA sector %d for %s isn't dirty, so nothing to store in external (meta) storage",
			index, lba.vdiskID)
		return // only store a dirty sector
	}

	var buffer bytes.Buffer
	if err = sector.Write(&buffer); err != nil {
		err = fmt.Errorf("couldn't serialize evicted sector %d: %s", index, err)
		return
	}

	conn, err := lba.provider.MetaRedisConnection()
	if err != nil {
		return
	}
	defer conn.Close()

	_, err = conn.Do("HSET", lba.storageKey, index, buffer.Bytes())
	if err != nil {
		sector.UnsetDirty()
	}

	return
}

// Delete a single sector from the external storage.
func (lba *LBA) deleteSectorFromExternalStorage(index int64) (err error) {
	conn, err := lba.provider.MetaRedisConnection()
	if err != nil {
		return
	}
	defer conn.Close()

	_, err = conn.Do("HDEL", lba.storageKey, index)

	return
}

// flushError is a collection of errors received,
// send by the server as a reply on the commands that got flushed to it
type flushError []error

// Error implements Error.Error
func (e flushError) Error() (s string) {
	s = fmt.Sprintf("flush failed because of %d commands: ", len(e))
	for _, err := range e {
		s += `"` + err.Error() + `";`
	}
	return
}

// StorageKey returns the storage key that can/will be
// used to store the LBA data for the given vdiskID
func StorageKey(vdiskID string) string {
	return StorageKeyPrefix + vdiskID
}

const (
	// StorageKeyPrefix is the prefix used in StorageKey
	StorageKeyPrefix = "lba:"
)
