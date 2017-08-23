package hash

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	data := make([]byte, 4096)
	rand.Read(data)

	expectedMd5 := md5.Sum(data)
	expectedBlake := blake2b.Sum256(data)
	expectedSha256 := sha256.Sum256(data)

	tests := []struct {
		name     string
		typ      string
		expected []byte
	}{
		{"md5", TypeMD5, expectedMd5[:]},
		{"blake2256", TypeBlake2, expectedBlake[:]},
		{"sha256", TypeSHA256, expectedSha256[:]},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testHash(t, data, test.expected, test.typ)
		})
	}
}

func testHash(t *testing.T, data, expected []byte, typ string) {
	hasher, err := NewHasher(Config{
		Type: typ,
	})
	assert.Nil(t, err)

	assert.Equal(t, expected, hasher.Hash(data))
}
