package main

//NumberOfRecordsPerLBAShard is the fixed length of the LBAShards
const NumberOfRecordsPerLBAShard = 128

//LBAShard is a collection of 128 LBA Records (Hash)
type LBAShard [NumberOfRecordsPerLBAShard]*Hash

//NewLBAShard initializes a new LBAShard an returns a pointer to it
func NewLBAShard() *LBAShard {
	return &LBAShard{}
}

//An LBA implements the functionality to lookup block keys through the logical block index
type LBA []*LBAShard

//NewLBA creates a new LBA with enough shards to hold the requested numberOfBlocks
// TODO: this is a naive in memory implementation so we can continue testing,
//		 need to create a persistent implementation (issue #5)
func NewLBA(numberOfBlocks uint64) (lba *LBA) {
	numberOfShards := numberOfBlocks / NumberOfRecordsPerLBAShard
	//If the number of blocks is not aligned on the number of shards, add an extra one
	if (numberOfBlocks % NumberOfRecordsPerLBAShard) != 0 {
		numberOfShards++
	}

	l := make(LBA, numberOfShards)
	lba = &l
	return
}

//Set the content hash for a specific block
func (lba *LBA) Set(blockIndex int64, h *Hash) {
	shard := (*lba)[blockIndex/NumberOfRecordsPerLBAShard]
	if shard == nil {
		//TODO: make this thing thread safe
		//		(to be done in the same feature work where
		//		 we make the lba content persistent) (issue #5)
		shard = NewLBAShard()
		(*lba)[blockIndex/NumberOfRecordsPerLBAShard] = shard
	}
	(*shard)[blockIndex%NumberOfRecordsPerLBAShard] = h
}

//Get returns the hash for a block, nil if no hash registered
func (lba *LBA) Get(blockIndex int64) (h *Hash) {
	shard := (*lba)[blockIndex/NumberOfRecordsPerLBAShard]
	if shard != nil {
		h = (*shard)[blockIndex%NumberOfRecordsPerLBAShard]
	}
	return
}
