package zerodisk

import (
	"fmt"
)

func ExampleVersionFromString() {
	v, err := VersionFromString("1.2.3-alpha")
	if err != nil {
		panic(err)
	}

	fmt.Println(v.Number.Major())
	fmt.Println(v.Number.Minor())
	fmt.Println(v.Number.Patch())
	fmt.Println(v.Label)

	// Output: 1
	// 2
	// 3
	// alpha
}
