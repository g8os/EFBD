package lba

import (
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
