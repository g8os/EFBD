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
