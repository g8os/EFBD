package zerodisk

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashBytes(t *testing.T) {
	data := make([]byte, 435)
	rand.Read(data)
	h := HashBytes(data)
	if assert.NotNil(t, h, "Nil hash returned from the hashfunction") {
		assert.False(t, h.Equals(NilHash), "empty has returned")
	}
}

func TestHasher(t *testing.T) {
	assert := assert.New(t)

	data := make([]byte, 435)
	rand.Read(data)

	hasher, err := NewHasher()
	if assert.NoError(err) {
		assert.NotNil(hasher)
	}

	h := hasher.HashBytes(data)
	if assert.NotNil(h, "Nil hash returned from the hashfunction") {
		assert.False(h.Equals(NilHash), "empty has returned")
	}
}

func TestKeyedHasher(t *testing.T) {
	assert := assert.New(t)

	data := make([]byte, 435)
	rand.Read(data)

	h1, err := NewKeyedHasher([]byte("foo"))
	if assert.NoError(err) {
		assert.NotNil(h1)
	}
	h2, err := NewKeyedHasher([]byte("bar"))
	if assert.NoError(err) {
		assert.NotNil(h2)
	}

	hash1 := h1.HashBytes(data)
	if assert.NotNil(hash1, "Nil hash returned from the hashfunction") {
		assert.Len(hash1, HashSize)
		assert.NotEqual(NilHash, hash1, "empty has returned")
	}
	hash2 := h2.HashBytes(data)
	if assert.NotNil(hash2, "Nil hash returned from the hashfunction") {
		assert.Len(hash2, HashSize)
		assert.NotEqual(NilHash, hash2, "empty has returned")
	}

	assert.NotEqual(hash1, hash2)
}

func TestNilHash(t *testing.T) {
	assert.Len(t, NilHash, HashSize)
	assert.True(t, NewHash().Equals(NilHash))
}

func BenchmarkHash_4k(b *testing.B) {
	benchmarkHash(b, 4*1024)
}

func BenchmarkHash_16k(b *testing.B) {
	benchmarkHash(b, 16*1024)
}

func BenchmarkHash_32k(b *testing.B) {
	benchmarkHash(b, 32*1024)
}

func benchmarkHash(b *testing.B, size int64) {
	in := make([]byte, size)
	b.SetBytes(size)

	var hash, prevHash []byte

	for i := 0; i < b.N; i++ {
		hash = HashBytes(in)
		if prevHash != nil && bytes.Compare(prevHash, hash) != 0 {
			b.Fatalf(
				"hash was expected to be %v, while received %v",
				prevHash, hash)
		} else if NilHash.Equals(hash) {
			b.Fatal("nil hash received")
		}
		prevHash = hash
	}
}
