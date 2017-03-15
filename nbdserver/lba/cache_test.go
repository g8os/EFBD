package lba

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateCacheInvalid(t *testing.T) {
	cache, err := newShardCache(0, nil)
	assert.Error(t, err, "given cache size is too small")
	assert.Nil(t, cache, "cache should be nil, in case of an error")
}

func TestCreateSingletonCache(t *testing.T) {
	cache, err := newShardCache(BytesPerShard, nil)
	if !assert.NoError(t, err, "given cache size is big enough for 1 shard") {
		return
	}
	assert.NotNil(t, cache, "cache should be non-nil, in case of no error")

	previousShard := newShard()
	var previousIndex int64

	// add a shard
	assert.False(t, cache.Add(previousIndex, previousShard), "no shard was there previously")
	// clear shards is possible
	cache.Clear(false)
	assert.False(t, cache.Add(previousIndex, previousShard), "no shard was there previously")

	// set an onEvict function so we can track
	var counter int
	cache.onEvict = func(index int64, shard *shard) {
		counter++
		assert.Equal(t, previousIndex, index)
		if assert.NotNil(t, shard, "shouldn't be nil") {
			assert.Equal(t, previousShard, shard)
		}
	}

	// keep adding new shards, so we can find if evicting happens correctly
	for currentIndex := previousIndex + 1; currentIndex < 128; currentIndex++ {
		s, ok := cache.Get(previousIndex)
		if assert.True(t, ok, "should be ok, as we have a shard available in cache") {
			assert.Equal(t, previousShard, s, "should return us the current shard")
		}

		shard := newShard()
		if assert.True(t, cache.Add(currentIndex, shard), "should evict previous shard") {
			_, ok = cache.Get(previousIndex)
			assert.False(t, ok, "shouldn't be ok, as that shard is no longer available")

			previousShard = shard
			previousIndex = currentIndex

			s, ok = cache.Get(currentIndex)
			if assert.True(t, ok, "should be ok, as we have a shard available in cache") {
				assert.Equal(t, shard, s, "should return us the current shard")
			}
		}
	}

	// should have evicted 127 times, with the 128th shard still in cache
	assert.Equal(t, counter, 127, "should have evicted 127 times")
}

func TestSerializeAndClearCache(t *testing.T) {
	shard1 := newShard()
	hash1 := HashBytes([]byte{4, 2})
	shard1.Set(1, hash1)

	shard2 := newShard()
	hash2 := HashBytes([]byte{2, 4})
	shard2.Set(2, hash2)

	cache, err := newShardCache(BytesPerShard*2, nil)
	if !assert.NoError(t, err) || !assert.NotNil(t, cache) {
		return
	}

	// no shards, shouldn't serialize anything
	assert.NoError(t, cache.Serialize(func(index int64, bytes []byte) error {
		t.Error("should not assert anything")
		return nil
	}))

	assert.False(t, cache.Add(1, shard1))

	assert.Error(t, cache.Serialize(nil), "no serialize function given")

	assert.Error(t, cache.Serialize(func(index int64, raw []byte) error {
		return errors.New("some error")
	}), "an error gets thrown in case the serialize function throws one")

	assert.NoError(t, cache.Serialize(func(index int64, raw []byte) error {
		assert.Equal(t, int64(1), index)
		assert.Len(t, raw, BytesPerShard)
		h1 := raw[HashSize : HashSize*2]
		assert.Equal(t, 0, bytes.Compare(hash1, h1))
		return nil
	}), "no error thrown")

	cache.onEvict = func(index int64, shard *shard) {
		assert.Equal(t, int64(1), index)
		if assert.NotNil(t, shard, "shouldn't be nil") {
			assert.Equal(t, shard1, shard)
		}
	}

	cache.Clear(true)

	// no shards, shouldn't serialize anything
	assert.NoError(t, cache.Serialize(func(index int64, bytes []byte) error {
		t.Error("should not assert anything")
		return nil
	}))

	// add 2 shards to the now empty cache
	assert.False(t, cache.Add(1, shard1))
	assert.False(t, cache.Add(2, shard2))

	assert.False(t, cache.Add(1, shard1), "shard already exists, so will not evict anything")
	assert.False(t, cache.Add(2, shard2), "shard already exists, so will not evict anything")

	assert.False(t, cache.Add(2, shard1), "shard already exists, so will simply overwrite")
	cache.onEvict = func(index int64, shard *shard) {
		assert.True(t, index == 1 || index == 2)
		if assert.NotNil(t, shard, "shouldn't be nil") {
			assert.Equal(t, shard1, shard)
		}
	}
	cache.Clear(true)

	// add 2 shards to the now empty cache
	assert.False(t, cache.Add(1, shard1))
	assert.False(t, cache.Add(2, shard2))

	cache.onEvict = func(index int64, shard *shard) {
		assert.Equal(t, int64(1), index)
		if assert.NotNil(t, shard, "shouldn't be nil") {
			assert.Equal(t, shard1, shard)
		}
	}
	// should evict, as shardIndex 3 is unique,
	// even though the shard value isn't
	assert.True(t, cache.Add(3, shard1))

	// variables used to test the Serialize Function Order
	s := shard1
	hash := hash1
	si := int64(3)
	offset := HashSize

	// should serialize shard1 first, as it's last added on index 3, and then shard2,
	// as that's the current order
	assert.NoError(t, cache.Serialize(func(index int64, raw []byte) error {
		assert.Equal(t, si, index)
		assert.Len(t, raw, BytesPerShard)
		h := raw[offset : offset+HashSize]
		assert.Equal(t, 0, bytes.Compare(hash, h))

		// set for next serialize call
		s = shard2
		hash = hash2
		si = int64(2)
		offset = HashSize * 2
		return nil
	}), "no error thrown")

	// if however we get shard2 at index 2, it will be pushed to the front,
	// and it will be serialized first
	receivedShard, ok := cache.Get(si)
	assert.True(t, ok, "should be ok")
	assert.Equal(t, s, receivedShard)

	assert.NoError(t, cache.Serialize(func(index int64, raw []byte) error {
		assert.Equal(t, si, index)
		assert.Len(t, raw, BytesPerShard)
		h := raw[offset : offset+HashSize]
		assert.Equal(t, 0, bytes.Compare(hash, h))

		// set for next serialize call
		s = shard1
		hash = hash1
		si = int64(3)
		offset = HashSize
		return nil
	}), "no error thrown")

	cache.onEvict = func(index int64, shard *shard) {
		t.Error("should not be called, as clear will be asked not to evict")
	}
	cache.Clear(false)
	cache.Serialize(func(index int64, raw []byte) error {
		t.Error("should not be called, as no shards should be cached")
		return nil
	})
}

func TestDeleteShardFromCache(t *testing.T) {
	var i int64
	s := newShard()
	if !assert.NotNil(t, s, "should never be nil") {
		return
	}

	cache, err := newShardCache(BytesPerShard, // only space for 1 shard
		func(index int64, shard *shard) { // onEvect function
			assert.Equal(t, i, index)
			assert.NotEqual(t, s, shard, "should be nil as it was deleted by the user")
			assert.Nil(t, shard, "should be nil as it was deleted by the user")
		})

	if !assert.NoError(t, err) || !assert.NotNil(t, cache) {
		return
	}

	assert.False(t, cache.Add(i, s))

	assert.False(t, cache.Delete(42), "42 is not the answer")
	assert.True(t, cache.Delete(i), "should be deleted")

	cache.Serialize(func(index int64, raw []byte) error {
		t.Error("should not be called, as no shards should be cached")
		return nil
	})

	cache.onEvict = func(index int64, shard *shard) { // onEvect function
		assert.Equal(t, i, index)
		assert.Equal(t, s, shard, "should be equal to the added shard")
		assert.NotNil(t, shard, "should not be nil as it was evicted")
	}

	assert.False(t, cache.Add(i, s))
	assert.True(t, cache.Add(42, s))
}
