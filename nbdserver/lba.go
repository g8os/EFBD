package main

import "github.com/garyburd/redigo/redis"

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
	shards    []*LBAShard
	redisPool *redis.Pool
}

//NewLBA creates a new LBA with enough shards to hold the requested numberOfBlocks
// TODO: this is a naive in memory implementation so we can continue testing,
//		 need to create a persistent implementation (issue #5)
func NewLBA(numberOfBlocks uint64, pool *redis.Pool) (lba *LBA) {
	numberOfShards := numberOfBlocks / NumberOfRecordsPerLBAShard
	//If the number of blocks is not aligned on the number of shards, add an extra one
	if (numberOfBlocks % NumberOfRecordsPerLBAShard) != 0 {
		numberOfShards++
	}
	lba = &LBA{
		shards:    make([]*LBAShard, numberOfShards),
		redisPool: pool,
	}
	return
}

//Set the content hash for a specific block.
// When a key is updated, the shard containing this blockindex is marked as dirty and will be
// stored in the external metadataserver when Flush is called.
func (lba *LBA) Set(blockIndex int64, h *Hash) {
	shard := lba.shards[blockIndex/NumberOfRecordsPerLBAShard]
	if shard == nil {
		//TODO: make this thing thread safe
		//		(to be done in the same feature work where
		//		 we make the lba content persistent) (issue #5)
		shard = NewLBAShard()
		lba.shards[blockIndex/NumberOfRecordsPerLBAShard] = shard
	}
	(*shard)[blockIndex%NumberOfRecordsPerLBAShard] = h
}

//Get returns the hash for a block, nil if no hash registered
// If the shard containing this blockindex is not present, it is fetched from the external metadaserver
func (lba *LBA) Get(blockIndex int64) (h *Hash) {
	shard := lba.shards[blockIndex/NumberOfRecordsPerLBAShard]
	if shard != nil {
		h = (*shard)[blockIndex%NumberOfRecordsPerLBAShard]
	}
	return
}

//Flush stores all dirty shards to the external metadaserver
func (lba *LBA) Flush() (err error) {
	return
}
