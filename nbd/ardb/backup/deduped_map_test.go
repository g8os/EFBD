package backup

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
)

func TestHashMapSerialization(t *testing.T) {
	const (
		hashCount = 1024 * 64
	)

	// create and set all hashes
	hashes := make(map[int64]zerodisk.Hash)
	for i := 0; i < hashCount; i++ {
		hash := zerodisk.NewHash()
		_, err := rand.Read(hash[:])
		if err != nil {
			t.Fatal(err)
		}
		hashes[int64(i)] = hash
	}

	buf := bytes.NewBuffer(nil)

	// serialize
	err := serializeHashes(hashes, buf)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf(
		"hashmap with %d hashes (%d bytes) is serialized into %d bytes",
		hashCount, ((zerodisk.HashSize + 4) * hashCount), len(buf.Bytes()))

	// deserialize
	outHashes, err := deserializeHashes(buf)
	if err != nil {
		t.Fatal(err)
	}

	for index, hash := range hashes {
		outHash, ok := outHashes[index]
		if !ok {
			t.Errorf("couldn't find hash %d in out hashmap", index)
			continue
		}
		assert.Equal(t, hash, outHash)
	}
}

func TestDedupedMapSerialization(t *testing.T) {
	dm := NewDedupedMap()

	const (
		hashCount = 1024 * 64
	)

	// set all hashes
	hashes := make([]zerodisk.Hash, hashCount)
	for i := 0; i < hashCount; i++ {
		hashes[i] = zerodisk.NewHash()
		_, err := rand.Read(hashes[i][:])
		if err != nil {
			t.Fatal(err)
		}

		assert.True(t, dm.SetHash(int64(i), hashes[i]))
	}

	// ensure all hashes were written
	for i := 0; i < hashCount; i++ {
		hash, ok := dm.GetHash(int64(i))
		if assert.True(t, ok) {
			assert.Equal(t, hashes[i], hash)
		}
	}

	var buf bytes.Buffer

	// serialize map
	err := dm.Serialize(&privKey, LZ4Compression, &buf)
	if !assert.NoError(t, err) {
		return
	}

	t.Logf(
		"deduped map of %d hashes (%d bytes) is serialized into %d bytes",
		hashCount, ((zerodisk.HashSize + 4) * hashCount), len(buf.Bytes()))

	// Deserialize map again
	dm, err = DeserializeDedupedMap(&privKey, LZ4Compression, &buf)
	if !assert.NoError(t, err) {
		return
	}

	// ensure all hashes are available
	for i := 0; i < hashCount; i++ {
		hash, ok := dm.GetHash(int64(i))
		if assert.True(t, ok) {
			assert.Equal(t, hashes[i], hash)
		}
	}
}
