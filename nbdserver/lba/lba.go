package lba

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/garyburd/redigo/redis"
)

//NewLBA creates a new LBA
func NewLBA(volumeID string, cacheLimitInBytes int64, pool *redis.Pool) (lba *LBA, err error) {
	lba = &LBA{
		redisPool: pool,
		volumeID:  volumeID,
	}
	lba.cache, err = newShardCache(cacheLimitInBytes, lba.onCacheEviction)

	return
}

// LBA implements the functionality to lookup block keys through the logical block index.
// The data is persisted to an external metadataserver in shards of n keys,
// where n = NumberOfRecordsPerLBAShard.
type LBA struct {
	lock  sync.Mutex
	cache *shardCache

	redisPool *redis.Pool
	volumeID  string
}

//Set the content hash for a specific block.
// When a key is updated, the shard containing this blockindex is marked as dirty and will be
// stored in the external metadataserver when Flush is called.
func (lba *LBA) Set(blockIndex int64, h Hash) (err error) {
	//TODO: let's see if we really need to lock on such a high level
	//		IF NOT, we need to make lba.shardCache thread safe!
	lba.lock.Lock()
	defer lba.lock.Unlock()

	//Fetch the appropriate shard
	shardIndex := blockIndex / NumberOfRecordsPerLBAShard
	shard, err := lba.getShard(shardIndex)
	if err != nil {
		return
	}
	if shard == nil {
		shard = newShard()
		// store the new shard in the cache,
		// otherwise it will be forgotten...
		lba.cache.Add(shardIndex, shard)
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
func (lba *LBA) Get(blockIndex int64) (h Hash, err error) {
	//TODO: let's see if we really need to lock on such a high level
	//		IF NOT, we need to make lba.shardCache thread safe!
	lba.lock.Lock()
	defer lba.lock.Unlock()

	shardIndex := blockIndex / NumberOfRecordsPerLBAShard
	shard, err := lba.getShard(shardIndex)
	if err != nil {
		return
	}
	if shard != nil {
		// get the hash
		hashIndex := blockIndex % NumberOfRecordsPerLBAShard
		h = shard.Get(hashIndex)
	}
	return
}

//Flush stores all dirty shards to the external metadaserver
func (lba *LBA) Flush() (err error) {
	//TODO: let's see if we really need to lock on such a high level
	//		IF NOT, we need to make lba.shardCache thread safe!
	lba.lock.Lock()
	defer lba.lock.Unlock()

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
		log.Printf("[ERROR] error during eviction of shard %d: %s", index, err)
	}
}

func (lba *LBA) getShardFromExternalStorage(index int64) (shard *shard, err error) {
	conn := lba.redisPool.Get()
	defer conn.Close()
	reply, err := conn.Do("HGET", lba.volumeID, index)
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
	conn := lba.redisPool.Get()
	defer conn.Close()

	//TODO: If a shard is competely empty,
	//      nil or contains only hashes from "0" content, delete it.

	if err = conn.Send("MULTI"); err != nil {
		return
	}

	lba.cache.Serialize(func(index int64, bytes []byte) (err error) {
		err = conn.Send("HSET", lba.volumeID, index, bytes)
		return
	})

	// Write all sets in output buffer to Redis at once
	_, err = conn.Do("EXEC")
	if err != nil {
		// no need to evict, already serialized them
		evict := false
		// clear cache, as we serialized them all
		lba.cache.Clear(evict)
	}
	return
}

func (lba *LBA) storeShardInExternalStorage(index int64, shard *shard) (err error) {
	if !shard.Dirty() {
		return // only store a dirty shard
	}

	var buffer bytes.Buffer
	if err = shard.Write(&buffer); err != nil {
		err = fmt.Errorf("couldn't serialize evicted shard %d: %s", index, err)
		return
	}

	conn := lba.redisPool.Get()
	defer conn.Close()

	_, err = conn.Do("HSET", lba.volumeID, index, buffer.Bytes())
	if err != nil {
		shard.UnsetDirty()
	}

	return
}

func (lba *LBA) deleteShardFromExternalStorage(index int64) (err error) {
	conn := lba.redisPool.Get()
	defer conn.Close()

	_, err = conn.Do("HDEL", lba.volumeID, index)

	return
}
