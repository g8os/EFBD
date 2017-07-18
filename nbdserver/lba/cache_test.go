package lba

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
)

func TestCreateCacheInvalid(t *testing.T) {
	cache, err := newSectorCache(0, nil)
	assert.Error(t, err, "given cache size is too small")
	assert.Nil(t, cache, "cache should be nil, in case of an error")
}

func TestCreateSingletonCache(t *testing.T) {
	cache, err := newSectorCache(BytesPerSector, nil)
	if !assert.NoError(t, err, "given cache size is big enough for 1 sector") {
		return
	}
	assert.NotNil(t, cache, "cache should be non-nil, in case of no error")

	previousSector := newSector()
	var previousIndex int64

	// add a sector
	assert.False(t, cache.Add(previousIndex, previousSector), "no sector was there previously")
	// clear sectors is possible
	cache.Clear(false)
	assert.False(t, cache.Add(previousIndex, previousSector), "no sector was there previously")

	// set an onEvict function so we can track
	var counter int
	cache.onEvict = func(index int64, sector *sector) {
		counter++
		assert.Equal(t, previousIndex, index)
		if assert.NotNil(t, sector, "shouldn't be nil") {
			assert.Equal(t, previousSector, sector)
		}
	}

	// keep adding new sectors, so we can find if evicting happens correctly
	for currentIndex := previousIndex + 1; currentIndex < 128; currentIndex++ {
		s, ok := cache.Get(previousIndex)
		if assert.True(t, ok, "should be ok, as we have a sector available in cache") {
			assert.Equal(t, previousSector, s, "should return us the current sector")
		}

		sector := newSector()
		if assert.True(t, cache.Add(currentIndex, sector), "should evict previous sector") {
			_, ok = cache.Get(previousIndex)
			assert.False(t, ok, "shouldn't be ok, as that sector is no longer available")

			previousSector = sector
			previousIndex = currentIndex

			s, ok = cache.Get(currentIndex)
			if assert.True(t, ok, "should be ok, as we have a sector available in cache") {
				assert.Equal(t, sector, s, "should return us the current sector")
			}
		}
	}

	// should have evicted 127 times, with the 128th sector still in cache
	assert.Equal(t, counter, 127, "should have evicted 127 times")
}

func TestSerializeAndClearCache(t *testing.T) {
	sector1 := newSector()
	hash1 := zerodisk.HashBytes([]byte{4, 2})
	sector1.Set(1, hash1)

	sector2 := newSector()
	hash2 := zerodisk.HashBytes([]byte{2, 4})
	sector2.Set(2, hash2)

	cache, err := newSectorCache(BytesPerSector*2, nil)
	if !assert.NoError(t, err) || !assert.NotNil(t, cache) {
		return
	}

	// no sectors, shouldn't serialize anything
	assert.NoError(t, cache.Serialize(func(index int64, bytes []byte) error {
		t.Error("should not assert anything")
		return nil
	}))

	assert.False(t, cache.Add(1, sector1))

	assert.Error(t, cache.Serialize(nil), "no serialize function given")

	assert.Error(t, cache.Serialize(func(index int64, raw []byte) error {
		return errors.New("some error")
	}), "an error gets thrown in case the serialize function throws one")

	assert.NoError(t, cache.Serialize(func(index int64, raw []byte) error {
		assert.Equal(t, int64(1), index)
		assert.Len(t, raw, BytesPerSector)
		h1 := raw[zerodisk.HashSize : zerodisk.HashSize*2]
		assert.Equal(t, 0, bytes.Compare(hash1, h1))
		return nil
	}), "no error thrown")

	cache.onEvict = func(index int64, sector *sector) {
		assert.Equal(t, int64(1), index)
		if assert.NotNil(t, sector, "shouldn't be nil") {
			assert.Equal(t, sector1, sector)
		}
	}

	cache.Clear(true)

	// no sectors, shouldn't serialize anything
	assert.NoError(t, cache.Serialize(func(index int64, bytes []byte) error {
		t.Error("should not assert anything")
		return nil
	}))

	// add 2 sectors to the now empty cache
	assert.False(t, cache.Add(1, sector1))
	assert.False(t, cache.Add(2, sector2))

	assert.False(t, cache.Add(1, sector1), "sector already exists, so will not evict anything")
	assert.False(t, cache.Add(2, sector2), "sector already exists, so will not evict anything")

	assert.False(t, cache.Add(2, sector1), "sector already exists, so will simply overwrite")
	cache.onEvict = func(index int64, sector *sector) {
		assert.True(t, index == 1 || index == 2)
		if assert.NotNil(t, sector, "shouldn't be nil") {
			assert.Equal(t, sector1, sector)
		}
	}
	cache.Clear(true)

	// add 2 sectors to the now empty cache
	assert.False(t, cache.Add(1, sector1))
	assert.False(t, cache.Add(2, sector2))

	cache.onEvict = func(index int64, sector *sector) {
		assert.Equal(t, int64(1), index)
		if assert.NotNil(t, sector, "shouldn't be nil") {
			assert.Equal(t, sector1, sector)
		}
	}
	// should evict, as sectorIndex 3 is unique,
	// even though the sector value isn't
	assert.True(t, cache.Add(3, sector1))

	// variables used to test the Serialize Function Order
	s := sector1
	hash := hash1
	si := int64(3)
	offset := zerodisk.HashSize

	// should serialize sector1 first, as it's last added on index 3, and then sector2,
	// as that's the current order
	assert.NoError(t, cache.Serialize(func(index int64, raw []byte) error {
		assert.Equal(t, si, index)
		assert.Len(t, raw, BytesPerSector)
		h := raw[offset : offset+zerodisk.HashSize]
		assert.Equal(t, 0, bytes.Compare(hash, h))

		// set for next serialize call
		s = sector2
		hash = hash2
		si = int64(2)
		offset = zerodisk.HashSize * 2
		return nil
	}), "no error thrown")

	// if however we get sector2 at index 2, it will be pushed to the front,
	// and it will be serialized first
	receivedSector, ok := cache.Get(si)
	assert.True(t, ok, "should be ok")
	assert.Equal(t, s, receivedSector)

	assert.NoError(t, cache.Serialize(func(index int64, raw []byte) error {
		assert.Equal(t, si, index)
		assert.Len(t, raw, BytesPerSector)
		h := raw[offset : offset+zerodisk.HashSize]
		assert.Equal(t, 0, bytes.Compare(hash, h))

		// set for next serialize call
		s = sector1
		hash = hash1
		si = int64(3)
		offset = zerodisk.HashSize
		return nil
	}), "no error thrown")

	cache.onEvict = func(index int64, sector *sector) {
		t.Error("should not be called, as clear will be asked not to evict")
	}
	cache.Clear(false)
	cache.Serialize(func(index int64, raw []byte) error {
		t.Error("should not be called, as no sectors should be cached")
		return nil
	})
}

func TestDeleteSectorFromCache(t *testing.T) {
	var i int64
	s := newSector()
	if !assert.NotNil(t, s, "should never be nil") {
		return
	}

	cache, err := newSectorCache(BytesPerSector, // only space for 1 sector
		func(index int64, sector *sector) { // onEvect function
			assert.Equal(t, i, index)
			assert.NotEqual(t, s, sector, "should be nil as it was deleted by the user")
			assert.Nil(t, sector, "should be nil as it was deleted by the user")
		})

	if !assert.NoError(t, err) || !assert.NotNil(t, cache) {
		return
	}

	assert.False(t, cache.Add(i, s))

	assert.False(t, cache.Delete(42), "42 is not the answer")
	assert.True(t, cache.Delete(i), "should be deleted")

	cache.Serialize(func(index int64, raw []byte) error {
		t.Error("should not be called, as no sectors should be cached")
		return nil
	})

	cache.onEvict = func(index int64, sector *sector) { // onEvect function
		assert.Equal(t, i, index)
		assert.Equal(t, s, sector, "should be equal to the added sector")
		assert.NotNil(t, sector, "should not be nil as it was evicted")
	}

	assert.False(t, cache.Add(i, s))
	assert.True(t, cache.Add(42, s))
}
