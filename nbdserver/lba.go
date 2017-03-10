package main

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/garyburd/redigo/redis"
)

//NumberOfRecordsPerLBAShard is the fixed length of the LBAShards
const NumberOfRecordsPerLBAShard = 128

//LBAShard is a collection of 128 LBA Records (Hash)
type LBAShard [NumberOfRecordsPerLBAShard]*Hash

//NewLBAShard initializes a new LBAShard an returns a pointer to it
func NewLBAShard() *LBAShard {
	return &LBAShard{}
}

// LBA implements the functionality to lookup block keys through the logical block index.
// The data is persisted to an external metadataserver in shards of 128 keys.
type LBA struct {
	lock        sync.Mutex
	shards      []*LBAShard
	dirtyShards map[int64]*LBAShard

	redisPool *redis.Pool
	volumeID  string
}

//NewLBA creates a new LBA with enough shards to hold the requested numberOfBlocks
func NewLBA(volumeID string, numberOfBlocks uint64, pool *redis.Pool) (lba *LBA) {
	numberOfShards := numberOfBlocks / NumberOfRecordsPerLBAShard
	//If the number of blocks is not aligned on the number of shards, add an extra one
	if (numberOfBlocks % NumberOfRecordsPerLBAShard) != 0 {
		numberOfShards++
	}
	lba = &LBA{
		shards:      make([]*LBAShard, numberOfShards),
		dirtyShards: make(map[int64]*LBAShard),
		redisPool:   pool,
		volumeID:    volumeID,
	}
	return
}

//Set the content hash for a specific block.
// When a key is updated, the shard containing this blockindex is marked as dirty and will be
// stored in the external metadataserver when Flush is called.
func (lba *LBA) Set(blockIndex int64, h *Hash) (err error) {
	//TODO: let's see if we really need to lock on such a high level
	lba.lock.Lock()
	defer lba.lock.Unlock()

	//Fetch the appropriate shard
	shardIndex := blockIndex / NumberOfRecordsPerLBAShard
	shard := lba.shards[shardIndex]
	if shard == nil {
		shard, err = lba.getShardFromExternalStorage(shardIndex)
		if err != nil {
			return
		}
		if shard == nil {
			shard = NewLBAShard()
		}
		lba.shards[shardIndex] = shard
	}
	//Update the hash
	(*shard)[blockIndex%NumberOfRecordsPerLBAShard] = h
	//Mark the shard as dirty
	lba.dirtyShards[shardIndex] = shard
	return
}

//Get returns the hash for a block, nil if no hash registered
// If the shard containing this blockindex is not present, it is fetched from the external metadaserver
func (lba *LBA) Get(blockIndex int64) (h *Hash, err error) {
	//TODO: let's see if we really need to lock on such a high level
	lba.lock.Lock()
	defer lba.lock.Unlock()

	shardIndex := blockIndex / NumberOfRecordsPerLBAShard
	shard := lba.shards[shardIndex]
	if shard == nil {
		shard, err = lba.getShardFromExternalStorage(shardIndex)
		if err != nil {
			return
		}
		lba.shards[shardIndex] = shard
	}
	if shard != nil {
		h = (*shard)[blockIndex%NumberOfRecordsPerLBAShard]
	}
	return
}

//Flush stores all dirty shards to the external metadaserver
func (lba *LBA) Flush() (err error) {
	//TODO: let's see if we really need to lock on such a high level
	lba.lock.Lock()
	defer lba.lock.Unlock()

	lba.storeShardsInExternalStorage(lba.dirtyShards)

	lba.dirtyShards = make(map[int64]*LBAShard)

	return
}

func (lba *LBA) createShardKey(shardIndex int64) string {
	return fmt.Sprintf("%s:%d", lba.volumeID, shardIndex)
}

func (lba *LBA) storeShardsInExternalStorage(shards map[int64]*LBAShard) (err error) {
	conn := lba.redisPool.Get()
	defer conn.Close()

	var key string
	var h *Hash
	var nilHash Hash
	var buffer bytes.Buffer

	// Start Pipe, so that all operations are piped
	if err = conn.Send("MULTI"); err != nil {
		return
	}

	// Collect all sets in output buffer of Redis
	for shardIndex, shard := range shards {
		buffer.Reset()

		key = lba.createShardKey(shardIndex)
		for _, h = range *shard {
			if h == nil {
				if _, err = buffer.Write(nilHash[:]); err != nil {
					return
				}
			} else {
				if _, err = buffer.Write(h[:]); err != nil {
					return
				}
			}
		}

		if err = conn.Send("SET", key, buffer.Bytes()); err != nil {
			return
		}
	}

	// Write all sets in output buffer to Redis at once
	_, err = conn.Do("EXEC")
	return
}

func (lba *LBA) getShardFromExternalStorage(shardIndex int64) (shard *LBAShard, err error) {
	key := lba.createShardKey(shardIndex)

	conn := lba.redisPool.Get()
	defer conn.Close()
	reply, err := conn.Do("GET", key)
	if err != nil || reply == nil {
		return
	}

	shardBytes, err := redis.Bytes(reply, err)
	if err != nil {
		return
	}
	shard = &LBAShard{}
	for i := 0; i < NumberOfRecordsPerLBAShard; i++ {
		var h Hash
		copy(h[:], shardBytes[i*HashSize:])
		(*shard)[i] = &h
	}
	return
}
