package hash

import (
	"github.com/minio/blake2b-simd"
)

func blake2bHash(plain []byte) []byte {
	// hashed := blake2b.Sum256(plain)
	// return hashed[:]
	h := blake2b.New256()
	h.Write(plain)
	return h.Sum(nil)
}
