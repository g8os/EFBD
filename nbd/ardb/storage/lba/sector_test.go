package lba

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
)

func TestCreateSector(t *testing.T) {
	// creating a new sector is always possible
	sector := newSector()
	if assert.NotNil(t, sector, "should never be nil") {
		assert.False(t, sector.Dirty(), "fresh sector should be clean")
		assert.Equal(t, BytesPerSector, len(sector.hashes), "should hold enough for all hashes")
	}

	// will fail, as we need an array that contains all hashes of a sector
	sector, err := sectorFromBytes(nil)
	if assert.Error(t, err, "not a big enough array") {
		assert.Nil(t, sector, "on error sector should always be nil")
	}

	// will succeed, but should still be clean
	sector, err = sectorFromBytes(make([]byte, BytesPerSector))
	if assert.NoError(t, err, "big enough array, should be ok") &&
		assert.NotNil(t, sector, "non-error should never return nil") {
		assert.False(t, sector.Dirty(), "fresh sector should be clean")

		// getting a sector will keep it clean
		for i := int64(0); i < NumberOfRecordsPerLBASector; i++ {
			h := sector.Get(i) // NOTE: there is no out-of-range protection
			assert.Equal(t, zerodisk.NilHash, h, "created from bytes, contain all nil hashes")
		}

		// setting a sector will make it dirty though
		sector.Set(0, zerodisk.HashBytes(nil))

		assert.True(t, sector.Dirty(), "modified sector should be dirty")
		h := sector.Get(0) // NOTE: there is no out-of-range protection
		if assert.NotEmpty(t, h, "created from bytes, no hash should be nil") {
			assert.NotEqual(t, zerodisk.NilHash, h, "should be not equal to the NilHash")
		}

		// a sector can be marked non-dirty by the user,
		// typically this is done after it is been written to persistent memory
		sector.UnsetDirty()
		assert.False(t, sector.Dirty(), "sector should now be clean")

		// a sector can be serialized, used to write it to persistent memory
		var buffer bytes.Buffer
		// Write can only fail in case the given writer fails,
		// which a bytes buffer should never do?!
		if assert.NoError(t, sector.Write(&buffer), "should not fail") {
			bytes := buffer.Bytes()
			assert.Len(t, bytes, BytesPerSector,
				"should be exactly the length of the space it requires to store hashes")

			// now we create a new sector, with those bytes, as a test
			sector, err = sectorFromBytes(bytes)
			if assert.NoError(t, err, "big enough array, should be ok") &&
				assert.NotNil(t, sector, "non-error should never return nil") {
				assert.False(t, sector.Dirty(), "fresh sector should be clean")

				// first sector should still not be equal
				f := sector.Get(0) // NOTE: there is no out-of-range protection
				if assert.NotNil(t, f, "created from bytes, but with noting in, so it's all nil") {
					assert.Equal(t, h, f, "should be equal to the hash written earlier")
				}

				// getting a sector will keep it clean
				for i := int64(1); i < NumberOfRecordsPerLBASector; i++ {
					h := sector.Get(i) // NOTE: there is no out-of-range protection
					assert.Equal(t, zerodisk.NilHash, h, "created from bytes, contain all nil hashes")
				}
			}
		}
	}
}
