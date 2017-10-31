package lba

import (
	"sync"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/errors"
)

const (
	// MinimumBucketSizeLimit defines how small the cache limit for the LBA
	// and thus a bucket can be at its extreme. Bigger is better.
	MinimumBucketSizeLimit = BytesPerSector * 8
)

// NewLBA creates a new LBA
func NewLBA(cacheLimitInBytes int64, storage SectorStorage) (*LBA, error) {
	if cacheLimitInBytes < MinimumBucketSizeLimit {
		return nil, errors.Newf(
			"sectorCache requires at least %d bytes",
			MinimumBucketSizeLimit,
		)
	}
	if storage == nil {
		return nil, errors.New("LBA requires a non-nil storage")
	}

	// compute bucket config variables
	bucketCount := cacheLimitInBytes / MinimumBucketSizeLimit
	if bucketCount > maxNumberOfSectorBuckets {
		bucketCount = maxNumberOfSectorBuckets
	}
	bucketLimitInBytes := cacheLimitInBytes / bucketCount

	// create all buckets
	buckets := make([]*sectorBucket, bucketCount)
	for index := range buckets {
		buckets[index] = newSectorBucket(bucketLimitInBytes, storage)
	}

	// create the LBA itself
	return &LBA{
		buckets:     buckets,
		bucketCount: int32(bucketCount),
	}, nil
}

// LBA implements the functionality to lookup block keys through the logical block index.
// The data is persisted to an external metadataserver in sectors of n keys,
// where n = NumberOfRecordsPerLBASector.
type LBA struct {
	buckets     []*sectorBucket
	bucketCount int32
}

// Set the content hash for a specific block.
// When a key is updated, the sector containing this blockindex is marked as dirty and will be
// stored in the external metadataserver when Flush is called,
// or when the its getting evicted from the cache due to space limitations.
func (lba *LBA) Set(blockIndex int64, h zerodisk.Hash) error {
	bucket := lba.getBucket(blockIndex)
	return bucket.SetHash(blockIndex, h)
}

// Delete the content hash for a specific block.
// When a key is updated, the sector containing this blockindex is marked as dirty and will be
// stored in the external metadaserver when Flush is called,
// or when the its getting evicted from the cache due to space limitations.
// Deleting means actually that the nilhash will be set for this blockindex.
func (lba *LBA) Delete(blockIndex int64) error {
	return lba.Set(blockIndex, nil)
}

// Get returns the hash for a block, nil if no hash is registered.
// If the sector containing this blockindex is not present, it is fetched from the external metadaserver
func (lba *LBA) Get(blockIndex int64) (zerodisk.Hash, error) {
	bucket := lba.getBucket(blockIndex)
	return bucket.GetHash(blockIndex)
}

// Flush stores all dirty sectors to the external storage
func (lba *LBA) Flush() error {
	var wg sync.WaitGroup
	errs := errors.NewErrorSlice()

	for _, bucket := range lba.buckets {
		wg.Add(1)
		bucket := bucket
		go func() {
			defer wg.Done()
			errs.Add(bucket.Flush())
		}()
	}

	wg.Wait()
	return errs.AsError()
}

func (lba *LBA) getBucket(blockIndex int64) *sectorBucket {
	bucketIndex := bucketIndex(blockIndex, lba.bucketCount)
	return lba.buckets[bucketIndex]
}

func bucketIndex(blockIndex int64, bucketCount int32) int {
	sectorIndex := blockIndex / NumberOfRecordsPerLBASector
	return int(jumpConsistentHash(uint64(sectorIndex), bucketCount))
}

// jumpConsistentHash taken from https://arxiv.org/pdf/1406.2294.pdf
func jumpConsistentHash(key uint64, numBuckets int32) int32 {
	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

const (
	// maxNumberOfSectorBuckets is the maximum number of buckets we'll use
	// TODO: define this number of buckets with some more thought
	maxNumberOfSectorBuckets = 64
)
