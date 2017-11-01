package storage

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/nbd/ardb/storage/lba"
	"github.com/zero-os/0-Disk/redisstub"
)

func TestLBAWithEmptyLBASectorStorage(t *testing.T) {
	const (
		bucketCount   = 8
		lbaCacheLimit = lba.MinimumBucketSizeLimit * bucketCount
		sectors       = 8
	)

	require := require.New(t)

	cluster := redisstub.NewUniCluster(true)
	require.NotNil(cluster)
	defer cluster.Close()

	storage := newLBASectorStorage("foo", cluster)
	require.NotNil(storage)

	lba, err := lba.NewLBA(lbaCacheLimit, storage)
	require.NoError(err)
	require.NotNil(lba)

	testLBAWithEmptyStorage(t, sectors, bucketCount, lba)
}

func testLBAWithEmptyStorage(t *testing.T, sectors, buckets int64, vlba *lba.LBA) {
	var (
		sectorCount = buckets * sectors * 5
	)

	// create all sectors randomly
	allSectors := make([][]byte, sectorCount)
	for i := range allSectors {
		allSectors[i] = make([]byte, lba.BytesPerSector)
		rand.Read(allSectors[i])
		// set some hashes to nil
		for h := 0; h < lba.NumberOfRecordsPerLBASector; h++ {
			if h%25 == 0 {
				offset := h * zerodisk.HashSize
				copy(allSectors[i][offset:], zerodisk.NilHash)
			}
		}
	}

	// set all hashes, should succeed
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < lba.NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*lba.NumberOfRecordsPerLBASector + hashIndex)
			offset := hashIndex * zerodisk.HashSize
			err := vlba.Set(index, zerodisk.Hash(sector[offset:offset+zerodisk.HashSize]))
			if err != nil {
				t.Fatal(sectorIndex, hashIndex, err)
			}
		}
	}

	// now flush to be sure all content is gone
	err := vlba.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// now get all hashes, and make sure they are correct, should be fine
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < lba.NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*lba.NumberOfRecordsPerLBASector + hashIndex)
			hash, err := vlba.Get(index)
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
		allSectors[i] = make([]byte, lba.BytesPerSector)
		rand.Read(allSectors[i])
		// set some hashes to nil
		for h := 0; h < lba.NumberOfRecordsPerLBASector; h++ {
			if h%25 == 0 {
				offset := h * zerodisk.HashSize
				copy(allSectors[i][offset:], zerodisk.NilHash)
			}
		}
	}

	// set all hashes, should succeed
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < lba.NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*lba.NumberOfRecordsPerLBASector + hashIndex)
			offset := hashIndex * zerodisk.HashSize
			err := vlba.Set(index, zerodisk.Hash(sector[offset:offset+zerodisk.HashSize]))
			if err != nil {
				t.Fatal(sectorIndex, hashIndex, err)
			}
		}
	}

	// now flush to be sure all content is gone
	err = vlba.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// now get all hashes, and make sure they are correct, should be fine
	for sectorIndex, sector := range allSectors {
		for hashIndex := 0; hashIndex < lba.NumberOfRecordsPerLBASector; hashIndex++ {
			index := int64(sectorIndex*lba.NumberOfRecordsPerLBASector + hashIndex)
			hash, err := vlba.Get(index)
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
