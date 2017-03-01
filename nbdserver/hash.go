package main

import "github.com/minio/blake2b-simd"

const (
	//HashSize is the length of a hash in bytes
	HashSize = 32
)

//Hash is just a pointer to a bytearray of size HashSize
type Hash [HashSize]byte

// HashBytes takes a byte slice .
func HashBytes(data []byte) *Hash {
	b := Hash(blake2b.Sum256(data))
	return &b
}

//Equals returns true if two hashes are the same
func (h *Hash) Equals(compareTo *Hash) bool {
	return *h == *compareTo
}
