package zerodisk_test

import (
	"encoding/hex"
	"fmt"

	"github.com/zero-os/0-Disk"
)

func ExampleHashBytes() {
	// given we have two sets of data ...
	dataA := []byte("data to hash")
	dataB := []byte("other data to hash")

	// we can obtain obtain hashes
	hexA := zerodisk.HashBytes(dataA)
	printHex(hexA)

	hexB := zerodisk.HashBytes(dataB)
	printHex(hexB)

	// we can compare hashes
	sameHash := hexA.Equals(hexA)
	fmt.Printf("it is %v that hashes are equal\n", sameHash)

	sameHash = hexA.Equals(hexB)
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

	// we can define a new instance of default hasher
	hasher, err := zerodisk.NewHasher()
	panicOnError(err)

	// hasher is used to hash the data
	h := hasher.HashBytes(data)
	printHex(h)

	// Output:
	// ae1c89d781f63c4dd6c8ec4703b711bed45966af278446749dbe0eed34eaedf3
}

func ExampleNewKeyedHasher() {
	// given we have data and key ...
	data := []byte("data to hash")
	key := []byte("key")

	// we can define a new instance of default keyed hasher
	hasher, err := zerodisk.NewKeyedHasher(key)
	panicOnError(err)

	// hasher is used to hash the data
	h := hasher.HashBytes(data)
	printHex(h)
	// Output:
	// 5e09ed568017f03f66d6cca8c37272d0c55be86e9c27cf459721037c8fc3b5bb
}

func ExampleNewVersion() {
	// we can define a new version of the zerodisk modules
	label := zerodisk.VersionLabel{'b', 'e', 't', 'a', '-', '2'}
	ver := zerodisk.NewVersion(2, 3, 4, &label)

	fmt.Println(ver)
	// Output: 2.3.4-beta-2
}

func ExampleVersion_Compare() {
	// given we have several versions of the zerodisk modules
	versA := zerodisk.NewVersion(2, 3, 4, nil)
	versB := zerodisk.NewVersion(2, 3, 4, nil)
	versC := zerodisk.NewVersion(1, 1, 0, nil)

	// we can compare versions
	diff := versA.Compare(versB)
	fmt.Println(diff)

	diff = versB.Compare(versC)
	fmt.Println(diff)

	diff = versC.Compare(versA)
	fmt.Println(diff)
	// Output:
	// 0
	// 1
	// -1
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func printHex(h zerodisk.Hash) {
	str := hex.EncodeToString(h)
	fmt.Println(str)
}
