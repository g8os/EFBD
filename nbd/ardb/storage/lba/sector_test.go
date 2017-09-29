package lba

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
)

func TestCreateSector(t *testing.T) {
	// creating a new Sector is always possible
	sector := NewSector()
	if assert.NotNil(t, sector, "should never be nil") {
		assert.False(t, sector.Dirty(), "fresh Sector should be clean")
		assert.Equal(t, BytesPerSector, len(sector.hashes), "should hold enough for all hashes")
	}

	// will fail, as we need an array that contains all hashes of a Sector
	sector, err := SectorFromBytes(nil)
	if assert.Error(t, err, "not a big enough array") {
		assert.Nil(t, sector, "on error Sector should always be nil")
	}

	// will succeed, but should still be clean
	sector, err = SectorFromBytes(make([]byte, BytesPerSector))
	if assert.NoError(t, err, "big enough array, should be ok") &&
		assert.NotNil(t, sector, "non-error should never return nil") {
		assert.False(t, sector.Dirty(), "fresh Sector should be clean")

		// getting a Sector will keep it clean
		for i := int64(0); i < NumberOfRecordsPerLBASector; i++ {
			h := sector.Get(i) // NOTE: there is no out-of-range protection
			assert.Nil(t, h, "created from bytes, contain all nil hashes")
		}

		// setting a Sector will make it dirty though
		sector.Set(0, zerodisk.HashBytes(nil))

		assert.True(t, sector.Dirty(), "modified Sector should be dirty")
		h := sector.Get(0) // NOTE: there is no out-of-range protection
		if assert.NotEmpty(t, h, "created from bytes, no hash should be nil") {
			if !assert.NotNil(t, h, "should be nil") {
				assert.NotEqual(t, zerodisk.NilHash, h, "should be not equal to the NilHash")
			}
		}

		// a Sector can be marked non-dirty by the user,
		// typically this is done after it is been written to persistent memory
		sector.UnsetDirty()
		assert.False(t, sector.Dirty(), "Sector should now be clean")

		// the internal slice of a buffer can be received,
		// but this should be done with care!
		bytes := sector.Bytes()
		if assert.NotNil(t, bytes, "should not be nil") {
			assert.Len(t, bytes, BytesPerSector,
				"should be exactly the length of the space it requires to store hashes")

			// now we create a new Sector, with those bytes, as a test
			sector, err = SectorFromBytes(bytes)
			if assert.NoError(t, err, "big enough array, should be ok") &&
				assert.NotNil(t, sector, "non-error should never return nil") {
				assert.False(t, sector.Dirty(), "fresh Sector should be clean")

				// first Sector should still not be equal
				f := sector.Get(0) // NOTE: there is no out-of-range protection
				if assert.NotNil(t, f, "created from bytes, but with noting in, so it's all nil") {
					assert.Equal(t, h, f, "should be equal to the hash written earlier")
				}

				// getting a Sector will keep it clean
				for i := int64(1); i < NumberOfRecordsPerLBASector; i++ {
					h := sector.Get(i) // NOTE: there is no out-of-range protection
					assert.Nil(t, h, "created from bytes, contain all nil hashes")
				}
			}
		}
	}
}

func TestSectorSetAndGet(t *testing.T) {
	sector := NewSector()
	if sector == nil {
		t.Fatal("couldn't create Sector")
	}

	hashes := make([]zerodisk.Hash, NumberOfRecordsPerLBASector)
	for i := range hashes {
		hashes[i] = zerodisk.NewHash()
		rand.Read(hashes[i])
	}

	// set all hashes
	for i, h := range hashes {
		sector.Set(int64(i), h)
	}

	// get all hashes, should be equal to what we created
	for i, h := range hashes {
		hash := sector.Get(int64(i))
		if !h.Equals(hash) {
			t.Fatalf("unexpected hash (%d): found %v, expected %v",
				i, h, hash)
		}
	}

	// check bytes
	allHashes := sector.Bytes()
	for i, h := range hashes {
		offset := i * zerodisk.HashSize
		hash := zerodisk.Hash(allHashes[offset : offset+zerodisk.HashSize])
		if !h.Equals(hash) {
			t.Fatalf("unexpected hash (%d): found %v, expected %v",
				i, h, hash)
		}
	}

	// now get a new Sector
	sector, err := SectorFromBytes(allHashes)
	if err != nil {
		t.Fatal(err)
	}
	if sector == nil {
		t.Fatal("couldn't create Sector")
	}

	// get all hashes, should be equal to what we created
	for i, h := range hashes {
		hash := sector.Get(int64(i))
		if !h.Equals(hash) {
			t.Fatalf("unexpected hash (%d): found %v, expected %v",
				i, h, hash)
		}
	}
}
