package zerodisk

import (
	"bytes"
	"hash"

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

// Hasher is the crypto hasher interface used,
// for all zerodisk crypto-hash purposes,
// where we want to reuse a hasher for some reason.
type Hasher interface {
	HashBytes([]byte) Hash
}

// NewHasher returns a new instance of the default hasher
// used in 0-Disk, using the given key.
func NewHasher() (Hasher, error) {
	return &hasher{blake2b.New256()}, nil
}

// NewKeyedHasher returns a new instance of the default keyed hasher
// used in 0-Disk, using the given key.
func NewKeyedHasher(key []byte) (Hasher, error) {
	cfg := &blake2b.Config{
		Size: HashSize,
		Key:  key,
	}
	internal, err := blake2b.New(cfg)
	if err != nil {
		return nil, err
	}

	return &hasher{internal}, nil
}

// hasher defines the default hasher used in 0-Disk.
type hasher struct {
	internal hash.Hash
}

// HashBytes takes a byte slice and returns a hashed version of it.
func (h *hasher) HashBytes(data []byte) Hash {
	h.internal.Reset()
	h.internal.Write(data)

	hash := NewHash()
	sum := h.internal.Sum(nil)
	copy(hash[:], sum[:HashSize])
	return hash
}

//Hash is just a bytearray of size HashSize
type Hash []byte

// HashBytes takes a byte slice and returns a hashed version of it.
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
	return h
}
