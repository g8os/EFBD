package zerodisk

import (
	"encoding/hex"
	"fmt"
)

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func ExampleHashBytes() {
	// given we have two sets of data ...
	data := []byte("data to hash")
	dataDiff := []byte("other data to hash")

	// we can obtain obtain hashes
	h := HashBytes(data)
	hDiff := HashBytes(dataDiff)

	str := hex.EncodeToString(h)
	strDiff := hex.EncodeToString(hDiff)

	fmt.Println(str)
	fmt.Println(strDiff)

	// we can compare hashes
	sameHash := h.Equals(h)
	fmt.Printf("it is %v that hashes are equal\n", sameHash)

	sameHash = h.Equals(hDiff)
	fmt.Printf("it is %v that hashes are equal\n", sameHash)
	// Output:
	// ae1c89d781f63c4dd6c8ec4703b711bed45966af278446749dbe0eed34eaedf3
	// 4154c68e4df38451a009232697d3da08cbc02aa411bb1e03f1006aa046a84bd4
	// it is true that hashes are equal
	// it is false that hashes are equal
}

func ExampleNewHasher() {
	// given we have data ...
	data := []byte("data to hash")

	// we can define a new unstance of default hasher
	hasher, err := NewHasher()
	panicOnError(err)

	// hasher is used to hash the data
	h := hasher.HashBytes(data)

	str := hex.EncodeToString(h)
	fmt.Println(str)
	// Output:
	// ae1c89d781f63c4dd6c8ec4703b711bed45966af278446749dbe0eed34eaedf3
}

func ExampleNewKeyedHasher() {
	// given we have data and key ...
	data := []byte("data to hash")
	key := []byte("key")

	// we can define a new unstance of default keyed hasher
	hasher, err := NewKeyedHasher(key)
	panicOnError(err)

	// hasher is used to hash the data
	h := hasher.HashBytes(data)
	str := hex.EncodeToString(h)

	fmt.Println(str)
	// Output:
	// 5e09ed568017f03f66d6cca8c37272d0c55be86e9c27cf459721037c8fc3b5bb
}
