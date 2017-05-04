package lba

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/g8os/blockstor"
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

// MetaRedisProvider is used by the LBA,
// to retreive a Redis Meta Connection
type MetaRedisProvider interface {
	MetaRedisConnection() (redis.Conn, error)
}

//NewLBA creates a new LBA
func NewLBA(vdiskID string, blockCount, cacheLimitInBytes int64, provider MetaRedisProvider) (lba *LBA, err error) {
	if provider == nil {
		return nil, errors.New("NewLBA requires a non-nil MetaRedisProvider")
	}

	muxCount := blockCount / NumberOfRecordsPerLBAShard
	if blockCount%NumberOfRecordsPerLBAShard > 0 {
		muxCount++
	}

	lba = &LBA{
		provider: provider,
		vdiskID:  vdiskID,
		shardMux: make([]sync.Mutex, muxCount),
	}

	lba.cache, err = newShardCache(cacheLimitInBytes, lba.onCacheEviction)

	return
}

// LBA implements the functionality to lookup block keys through the logical block index.
// The data is persisted to an external metadataserver in shards of n keys,
// where n = NumberOfRecordsPerLBAShard.
type LBA struct {
	cache *shardCache

	// One mutex per shard, allows us to only lock
	// on a per-shard basis. Even with 65k block, that's still only a ~500 element mutex array.
	// We stil need to lock on a per-shard basis,
	// as otherwise we might have a race condition where for example
	// 2 operations might create a new shard, and thus we would miss an operation.
	shardMux []sync.Mutex

	provider MetaRedisProvider
	vdiskID  string
}

//Set the content hash for a specific block.
// When a key is updated, the shard containing this blockindex is marked as dirty and will be
// stored in the external metadataserver when Flush is called.
func (lba *LBA) Set(blockIndex int64, h blockstor.Hash) (err error) {
	//Fetch the appropriate shard
	shard, err := func(shardIndex int64) (shard *shard, err error) {
		lba.shardMux[shardIndex].Lock()
		defer lba.shardMux[shardIndex].Unlock()

		shard, err = lba.getShard(shardIndex)
		if err != nil {
			return
		}
		if shard == nil {
			shard = newShard()
			// store the new shard in the cache,
			// otherwise it will be forgotten...
			lba.cache.Add(shardIndex, shard)
		}

		return
	}(blockIndex / NumberOfRecordsPerLBAShard)

	if err != nil {
		return
	}

	//Update the hash
	hashIndex := blockIndex % NumberOfRecordsPerLBAShard
	shard.Set(hashIndex, h)

	return
}

//Delete the content hash for a specific block.
// When a key is updated, the shard containing this blockindex is marked as dirty and will be
// stored in the external metadaserver when Flush is called
// Deleting means actually that the nilhash will be set for this blockindex.
func (lba *LBA) Delete(blockIndex int64) (err error) {
	err = lba.Set(blockIndex, nil)
	return
}

//Get returns the hash for a block, nil if no hash registered
// If the shard containing this blockindex is not present, it is fetched from the external metadaserver
func (lba *LBA) Get(blockIndex int64) (h blockstor.Hash, err error) {
	shard, err := func(shardIndex int64) (*shard, error) {
		lba.shardMux[shardIndex].Lock()
		defer lba.shardMux[shardIndex].Unlock()

		return lba.getShard(shardIndex)
	}(blockIndex / NumberOfRecordsPerLBAShard)

	if err != nil || shard == nil {
		return
	}

	// get the hash
	hashIndex := blockIndex % NumberOfRecordsPerLBAShard
	h = shard.Get(hashIndex)
	if h.Equals(blockstor.NilHash) {
		h = nil
	}

	return
}

//Flush stores all dirty shards to the external metadaserver
func (lba *LBA) Flush() (err error) {
	err = lba.storeCacheInExternalStorage()
	return
}

func (lba *LBA) getShard(index int64) (shard *shard, err error) {
	shard, ok := lba.cache.Get(index)
	if !ok {
		shard, err = lba.getShardFromExternalStorage(index)
		if err != nil {
			return
		}

		if shard != nil {
			lba.cache.Add(index, shard)
		}
	}

	return
}

// in case a shard gets evicted from cache,
// this method will be called, and we'll serialize the shard immediately,
// unless it isn't dirty
func (lba *LBA) onCacheEviction(index int64, shard *shard) {
	if !shard.Dirty() {
		return
	}

	var err error

	// the given shard can be nil in case it was deleted by the user,
	// in that case we will remove the shard from the external storage as well
	// otherwise we serialize the shard before it gets thrown into the void
	if shard != nil {
		err = lba.storeShardInExternalStorage(index, shard)
	} else {
		err = lba.deleteShardFromExternalStorage(index)
	}

	if err != nil {
		log.Infof("error during eviction of shard %d: %s", index, err)
	}
}

func (lba *LBA) getShardFromExternalStorage(index int64) (shard *shard, err error) {
	conn, err := lba.provider.MetaRedisConnection()
	if err != nil {
		return
	}
	defer conn.Close()
	reply, err := conn.Do("HGET", lba.vdiskID, index)
	if err != nil || reply == nil {
		return
	}

	shardBytes, err := redis.Bytes(reply, err)
	if err != nil {
		return
	}

	shard, err = shardFromBytes(shardBytes)
	return
}

func (lba *LBA) storeCacheInExternalStorage() (err error) {
	conn, err := lba.provider.MetaRedisConnection()
	if err != nil {
		return
	}
	defer conn.Close()

	var cmdCount int64
	lba.cache.Serialize(func(index int64, bytes []byte) (err error) {
		if bytes != nil {
			err = conn.Send("HSET", lba.vdiskID, index, bytes)
		} else {
			err = conn.Send("HDEL", lba.vdiskID, index)
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

func (lba *LBA) storeShardInExternalStorage(index int64, shard *shard) (err error) {
	if !shard.Dirty() {
		log.Debugf(
			"LBA shard %d for %s isn't dirty, so nothing to store in external (meta) storage",
			index, lba.vdiskID)
		return // only store a dirty shard
	}

	var buffer bytes.Buffer
	if err = shard.Write(&buffer); err != nil {
		err = fmt.Errorf("couldn't serialize evicted shard %d: %s", index, err)
		return
	}

	conn, err := lba.provider.MetaRedisConnection()
	if err != nil {
		return
	}
	defer conn.Close()

	_, err = conn.Do("HSET", lba.vdiskID, index, buffer.Bytes())
	if err != nil {
		shard.UnsetDirty()
	}

	return
}

func (lba *LBA) deleteShardFromExternalStorage(index int64) (err error) {
	conn, err := lba.provider.MetaRedisConnection()
	if err != nil {
		return
	}
	defer conn.Close()

	_, err = conn.Do("HDEL", lba.vdiskID, index)

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
