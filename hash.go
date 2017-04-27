package blockstor

import (
	"bytes"

	"github.com/minio/blake2b-simd"
)

const (
	//HashSize is the length of a hash in bytes
	HashSize = 32
)

var (
	//NilHash is a hash with only '0'bytes
	NilHash = NewHash()
)

//Hash is just a bytearray of size HashSize
type Hash []byte

// HashBytes takes a byte slice .
func HashBytes(data []byte) Hash {
	b := blake2b.Sum256(data)
	return b[:]
}

//NewHash initializes a new empty hash
func NewHash() (hash Hash) {
	hash = make([]byte, HashSize, HashSize)
	return
}

//Equals returns true if two hashes are the same
func (h Hash) Equals(compareTo Hash) bool {
	return bytes.Equal(h, compareTo)
}

//Bytes returns the hash as a slice of bytes
func (h Hash) Bytes() []byte {
	return h[:]
}
