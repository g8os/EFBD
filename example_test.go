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
func printHex(h Hash) {
	str := hex.EncodeToString(h)
	fmt.Println(str)
}

func ExampleHashBytes() {
	// given we have two sets of data ...
	data := []byte("data to hash")
	dataDiff := []byte("other data to hash")

	// we can obtain obtain hashes
	h := HashBytes(data)
	printHex(h)

	hDiff := HashBytes(dataDiff)
	printHex(hDiff)

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

	// we can define a new instance of default hasher
	hasher, err := NewHasher()
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
	hasher, err := NewKeyedHasher(key)
	panicOnError(err)

	// hasher is used to hash the data
	h := hasher.HashBytes(data)
	printHex(h)
	// Output:
	// 5e09ed568017f03f66d6cca8c37272d0c55be86e9c27cf459721037c8fc3b5bb
}

func ExampleNewVersion() {
	// given we have a new version of the zerodisk modules
	// defined by ...
	var (
		major, minor, patch uint8
		label               *VersionLabel
	)
	major, minor, patch, label = 2, 3, 4, versionLabel("beta-2")

	// we can define a new version of the zerodisk modules
	ver := NewVersion(major, minor, patch, label)

	fmt.Println(ver)
	// Output: 2.3.4-beta-2
}

func versionDiff(diff int) string {
	switch diff {
	case -1:
		return "Lower version"
	case 0:
		return "Same version"
	case 1:
		return "Higher version"
	default:
		return ""
	}
}
func ExampleVersion_Compare() {
	// given we have several versions of the zerodisk modules
	versA := NewVersion(2, 3, 4, versionLabel("beta-2"))
	versB := NewVersion(2, 3, 4, versionLabel("beta-2"))
	versC := NewVersion(1, 1, 0, versionLabel("beta-1"))

	// we can compare versions
	diff := versA.Compare(versB)
	fmt.Println(versionDiff(diff))

	diff = versB.Compare(versC)
	fmt.Println(versionDiff(diff))

	diff = versC.Compare(versA)
	fmt.Println(versionDiff(diff))
	// Output:
	// Same version
	// Higher version
	// Lower version
}
