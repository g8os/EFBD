package lba

import (
	"crypto/rand"
	mrand "math/rand"
	"testing"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/redisstub"
)

func TestLBAWithEmptyStubStorage(t *testing.T) {
	const (
		bucketCount = 8
		sectors     = 8
		bucketLimit = (sectors / 2) * BytesPerSector
	)

	lba := newLBAWithStorageFactory(bucketCount, bucketLimit, func() sectorStorage {
		return newStubSectorStorage()
	})
	if lba == nil {
		t.Fatal("lba is nil")
	}

	testLBAWithEmptyStorage(t, sectors, bucketCount, lba)
}

func TestLBAWithEmptyARDBStorage(t *testing.T) {
	const (
		bucketCount = 8
		sectors     = 8
		bucketLimit = (sectors / 2) * BytesPerSector
	)

	provider := redisstub.NewInMemoryRedisProvider(nil)
	defer provider.Close()

	lba := newLBAWithStorageFactory(bucketCount, bucketLimit, func() sectorStorage {
		return newARDBSectorStorage("foo", provider)
	})
	if lba == nil {
		t.Fatal("lba is nil")
	}

	testLBAWithEmptyStorage(t, sectors, bucketCount, lba)
}

func testLBAWithEmptyStorage(t *testing.T, sectors, buckets int64, lba *LBA) {
	var (
		sectorCount = buckets * sectors * 5
	)

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
			err := lba.Set(index, zerodisk.Hash(sector[offset:offset+zerodisk.HashSize]))
			if err != nil {
				t.Fatal(sectorIndex, hashIndex, err)
			}
		}
	}

	// now flush to be sure all content is gone
	err := lba.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// now get all hashes, and make sure they are correct, should be fine
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*NumberOfRecordsPerLBASector + hashIndex)
			hash, err := lba.Get(index)
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

	// do it all over again

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
			err := lba.Set(index, zerodisk.Hash(sector[offset:offset+zerodisk.HashSize]))
			if err != nil {
				t.Fatal(sectorIndex, hashIndex, err)
			}
		}
	}

	// now flush to be sure all content is gone
	err = lba.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// now get all hashes, and make sure they are correct, should be fine
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*NumberOfRecordsPerLBASector + hashIndex)
			hash, err := lba.Get(index)
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

func TestBucketIndex_1_Bucket(t *testing.T) {
	testBucketIndex(t, 1)
}

func TestBucketIndex_4_Buckets(t *testing.T) {
	testBucketIndex(t, 4)
}

func TestBucketIndex_15_Buckets(t *testing.T) {
	testBucketIndex(t, 15)
}

func TestBucketIndex_64_Buckets(t *testing.T) {
	testBucketIndex(t, 64)
}

func testBucketIndex(t *testing.T, bucketCount int32) {
	for i := 0; i < 16777216; i++ {
		x := mrand.Uint32()
		bi := bucketIndex(int64(x), bucketCount)
		if bi < 0 || bi >= int(bucketCount) {
			t.Fatal("invalid bucket index", x, bucketCount, bi)
		}
	}
}
