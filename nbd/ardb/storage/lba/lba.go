package lba

import (
	"errors"
	"fmt"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

const (
	// StorageKeyPrefix is the prefix used in StorageKey
	StorageKeyPrefix = "lba:"
	// MinimumBucketSizeLimit defines how small the cache limit for the LBA
	// and thus a bucket can be at its extreme. Bigger is better.
	MinimumBucketSizeLimit = BytesPerSector * 8
)

// StorageKey returns the LBA storage key used for a given deduped vdisk
func StorageKey(vdiskID string) string {
	return StorageKeyPrefix + vdiskID
}

// TODO
// + DEBUG storage, make unit tests pass again
// + Make static errors
// + ADd Bucket Unit tests

// NewLBA creates a new LBA
func NewLBA(vdiskID string, cacheLimitInBytes int64, provider ardb.MetadataConnProvider) (lba *LBA, err error) {
	if vdiskID == "" {
		return nil, errors.New("NewLBA requires non-empty vdiskID")
	}
	if provider == nil {
		return nil, errors.New("NewLBA requires a non-nil MetaRedisProvider")
	}
	if cacheLimitInBytes < MinimumBucketSizeLimit {
		return nil, fmt.Errorf(
			"sectorCache requires at least %d bytes", MinimumBucketSizeLimit)
	}

	bucketCount := cacheLimitInBytes / MinimumBucketSizeLimit
	if bucketCount > maxNumberOfSectorBuckets {
		bucketCount = maxNumberOfSectorBuckets
	}

	bucketLimitInBytes := cacheLimitInBytes / bucketCount
	storageKey := StorageKey(vdiskID)

	log.Debugf("creating LBA for vdisk %s with %d bucket(s)", vdiskID, bucketCount)

	return newLBAWithStorageFactory(bucketCount, bucketLimitInBytes, func() sectorStorage {
		return newARDBSectorStorage(storageKey, provider)
	}), nil
}

func newLBAWithStorageFactory(bucketCount, bucketLimitInBytes int64, factory func() sectorStorage) *LBA {
	buckets := make([]*sectorBucket, bucketCount)

	var storage sectorStorage
	for index := range buckets {
		storage = factory()
		buckets[index] = newSectorBucket(bucketLimitInBytes, storage)
	}

	return &LBA{
		buckets:     buckets,
		bucketCount: bucketCount,
	}
}

// LBA implements the functionality to lookup block keys through the logical block index.
// The data is persisted to an external metadataserver in sectors of n keys,
// where n = NumberOfRecordsPerLBASector.
type LBA struct {
	buckets     []*sectorBucket
	bucketCount int64
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

// Flush stores all dirty sectors to the external metadaserver
func (lba *LBA) Flush() error {
	var errors flushError
	for _, bucket := range lba.buckets {
		errors.AddError(bucket.Flush())
	}
	return errors.AsError()
}

func (lba *LBA) getBucket(blockIndex int64) *sectorBucket {
	sectorIndex := blockIndex / NumberOfRecordsPerLBASector
	bucketIndex := bucketHash(sectorIndex, lba.bucketCount)
	return lba.buckets[bucketIndex]
}

func bucketHash(x, numBuckets int64) int64 {
	key := uint64(x)

	var b int64 = -1
	var j int64

	for j < numBuckets {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int64(b)
}

const (
	// maxNumberOfSectorBuckets is the maximum number of buckets we'll use
	maxNumberOfSectorBuckets = 10
)
