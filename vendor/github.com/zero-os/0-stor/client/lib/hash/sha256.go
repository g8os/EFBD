package hash

import (
	"crypto/sha256"
)

func sha256Hash(plain []byte) []byte {
	sum := sha256.Sum256(plain)
	return sum[:]
}
