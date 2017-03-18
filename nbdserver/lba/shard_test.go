package lba

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateShard(t *testing.T) {
	// creating a new shard is always possible
	shard := newShard()
	if assert.NotNil(t, shard, "should never be nil") {
		assert.False(t, shard.Dirty(), "fresh shard should be clean")
		assert.Equal(t, BytesPerShard, len(shard.hashes), "should hold enough for all hashes")
	}

	// will fail, as we need an array that contains all hashes of a shard
	shard, err := shardFromBytes(nil)
	if assert.Error(t, err, "not a big enough array") {
		assert.Nil(t, shard, "on error shard should always be nil")
	}

	// will succeed, but should still be clean
	shard, err = shardFromBytes(make([]byte, BytesPerShard))
	if assert.NoError(t, err, "big enough array, should be ok") &&
		assert.NotNil(t, shard, "non-error should never return nil") {
		assert.False(t, shard.Dirty(), "fresh shard should be clean")

		// getting a shard will keep it clean
		for i := int64(0); i < NumberOfRecordsPerLBAShard; i++ {
			h := shard.Get(i) // NOTE: there is no out-of-range protection
			assert.Equal(t, nilHash, h, "created from bytes, contain all nil hashes")
		}

		// setting a shard will make it dirty though
		shard.Set(0, HashBytes(nil))
		assert.True(t, shard.Dirty(), "modified shard should be dirty")
		h := shard.Get(0) // NOTE: there is no out-of-range protection
		if assert.NotEmpty(t, h, "created from bytes, no hash should be nil") {
			assert.NotEqual(t, nilHash, h, "should be not equal to the nilhash")
		}

		// a shard can be marked non-dirty by the user,
		// typically this is done after it is been written to persistent memory
		shard.UnsetDirty()
		assert.False(t, shard.Dirty(), "shard should now be clean")

		// a shard can be serialized, used to write it to persistent memory
		var buffer bytes.Buffer
		// Write can only fail in case the given writer fails,
		// which a bytes buffer should never do?!
		if assert.NoError(t, shard.Write(&buffer), "should not fail") {
			bytes := buffer.Bytes()
			assert.Len(t, bytes, BytesPerShard,
				"should be exactly the length of the space it requires to store hashes")

			// now we create a new shard, with those bytes, as a test
			shard, err = shardFromBytes(bytes)
			if assert.NoError(t, err, "big enough array, should be ok") &&
				assert.NotNil(t, shard, "non-error should never return nil") {
				assert.False(t, shard.Dirty(), "fresh shard should be clean")

				// first shard should still not be equal
				f := shard.Get(0) // NOTE: there is no out-of-range protection
				if assert.NotEmpty(t, f, "created from bytes, no hash should be nil") {
					assert.Equal(t, h, f, "should be not equal to the hash written earlier")
				}

				// getting a shard will keep it clean
				for i := int64(1); i < NumberOfRecordsPerLBAShard; i++ {
					h := shard.Get(i) // NOTE: there is no out-of-range protection
					assert.Equal(t, nilHash, h, "created from bytes, contain all nil hashes")
				}
			}
		}
	}
}
