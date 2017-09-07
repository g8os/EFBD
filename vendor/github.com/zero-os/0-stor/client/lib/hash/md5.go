package hash

import (
	"crypto/md5"
)

func md5Hash(plain []byte) []byte {
	sum := md5.Sum(plain)
	return sum[:]

}
