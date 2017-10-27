package zerodisk

import (
	"encoding/hex"
	"fmt"
)

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
