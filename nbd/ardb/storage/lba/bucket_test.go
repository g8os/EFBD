package lba

import (
	"container/list"
	"crypto/rand"
	"testing"

	"github.com/zero-os/0-Disk/redisstub"

	"github.com/zero-os/0-Disk"
)

func TestBucketWithEmptyStorage(t *testing.T) {
	const (
		sectorCount       = 5
		bucketSectorCount = 2
		bucketSize        = bucketSectorCount * BytesPerSector
	)

	bucket := createTestSectorBucket(bucketSize, nil)
	if bucket == nil {
		t.Fatal("failed to create bucket")
	}

	// create all sectors randomly
	allSectors := make([][]byte, sectorCount)
	for i := range allSectors {
		allSectors[i] = make([]byte, BytesPerSector)
		rand.Read(allSectors[i])
		// set some hashes to nil
		for h := 0; h < NumberOfRecordsPerLBASector; h++ {
			if h%25 == 0 {
				offset := h * zerodisk.HashSize
				copy(allSectors[i][offset:], zerodisk.NilHash)
			}
		}
	}

	// set all hashes, should succeed
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*NumberOfRecordsPerLBASector + hashIndex)
			offset := hashIndex * zerodisk.HashSize
			err := bucket.SetHash(index, zerodisk.Hash(sector[offset:offset+zerodisk.HashSize]))
			if err != nil {
				t.Fatal(sectorIndex, hashIndex, err)
			}
		}
	}

	// now flush to be sure all content is gone
	err := bucket.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// now get all hashes, and make sure they are correct, should be fine
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*NumberOfRecordsPerLBASector + hashIndex)
			hash, err := bucket.GetHash(index)
			if err != nil {
				t.Fatal(sectorIndex, hashIndex, err)
			}

			offset := hashIndex * zerodisk.HashSize
			expectedHash := zerodisk.Hash(sector[offset : offset+zerodisk.HashSize])

			if !(expectedHash.Equals(hash) || (hash == nil && expectedHash.Equals(zerodisk.NilHash))) {
				t.Fatalf("unexpected hash (%d,%d): found %v, expected %v",
					sectorIndex, hashIndex, hash, expectedHash)
			}
		}
	}
}

func TestBucketWithEmptyARDBStorage(t *testing.T) {
	const (
		sectorCount       = 5
		bucketSectorCount = 2
		bucketSize        = bucketSectorCount * BytesPerSector
	)

	cluster := redisstub.NewUniCluster(false)
	defer cluster.Close()

	ardbStorage := ARDBSectorStorage("foo", cluster)

	bucket := createTestSectorBucket(bucketSize, ardbStorage)
	if bucket == nil {
		t.Fatal("failed to create bucket")
	}

	// create all sectors randomly
	allSectors := make([][]byte, sectorCount)
	for i := range allSectors {
		allSectors[i] = make([]byte, BytesPerSector)
		rand.Read(allSectors[i])
		// set some hashes to nil
		for h := 0; h < NumberOfRecordsPerLBASector; h++ {
			if h%25 == 0 {
				offset := h * zerodisk.HashSize
				copy(allSectors[i][offset:], zerodisk.NilHash)
			}
		}
	}

	// set all hashes, should succeed
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*NumberOfRecordsPerLBASector + hashIndex)
			offset := hashIndex * zerodisk.HashSize
			err := bucket.SetHash(index, zerodisk.Hash(sector[offset:offset+zerodisk.HashSize]))
			if err != nil {
				t.Fatal(sectorIndex, hashIndex, err)
			}
		}
	}

	// now flush to be sure all content is gone
	err := bucket.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// now get all hashes, and make sure they are correct, should be fine
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*NumberOfRecordsPerLBASector + hashIndex)
			hash, err := bucket.GetHash(index)
			if err != nil {
				t.Fatal(sectorIndex, hashIndex, err)
			}

			offset := hashIndex * zerodisk.HashSize
			expectedHash := zerodisk.Hash(sector[offset : offset+zerodisk.HashSize])

			if !(expectedHash.Equals(hash) || (hash == nil && expectedHash.Equals(zerodisk.NilHash))) {
				t.Fatalf("unexpected hash (%d,%d): found %v, expected %v",
					sectorIndex, hashIndex, hash, expectedHash)
			}
		}
	}
}

func TestBucketWithFullStorage(t *testing.T) {
	const (
		sectorCount       = 5
		bucketSectorCount = 2
		bucketSize        = bucketSectorCount * BytesPerSector
	)

	// create all sectors randomly
	allSectors := make(map[int64][]byte)
	for i := int64(0); i < sectorCount; i++ {
		allSectors[i] = make([]byte, BytesPerSector)
		rand.Read(allSectors[i])
	}

	bucket := createTestSectorBucket(bucketSize, &stubSectorStorage{
		sectors: allSectors,
	})
	if bucket == nil {
		t.Fatal("failed to create bucket")
	}

	// get all hashes, and make sure they are correct, should be fine
	for sectorIndex, sector := range allSectors {
		for hashIndex := int64(0); hashIndex < NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*NumberOfRecordsPerLBASector + hashIndex)
			hash, err := bucket.GetHash(index)
			if err != nil {
				t.Fatal(sectorIndex, hashIndex, err)
			}

			offset := hashIndex * zerodisk.HashSize
			expectedHash := zerodisk.Hash(sector[offset : offset+zerodisk.HashSize])

			if !expectedHash.Equals(hash) {
				t.Fatalf("unexpected hash (%d,%d): found %v, expected %v",
					sectorIndex, hashIndex, hash, expectedHash)
			}
		}
	}
}

func createTestSectorBucket(bucketSize int64, storage SectorStorage) *sectorBucket {
	size := bucketSize / BytesPerSector

	if storage == nil {
		storage = newStubSectorStorage()
	}

	return &sectorBucket{
		sectors:   make(map[int64]*list.Element, size),
		evictList: list.New(),
		size:      int(size),
		storage:   storage,
	}
}
