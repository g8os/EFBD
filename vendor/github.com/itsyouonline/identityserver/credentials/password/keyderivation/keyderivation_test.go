package keyderivation

import (
	"fmt"
	"testing"
)

func TestHash(t *testing.T) {
	password := "demo"
	fmt.Println("Password: ", password)
	key, err := Hash(password)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("Key: ", key)
}

func TestCheck(t *testing.T) {
	password := "demo"
	key := "$6$Iu14DOBmK4/JqCTd$E67.yXll739DjVacyQjulJ6bjN1GIwkHfd/Er4HpABMvpq4x0fpH8aOdOmUjVZSmr8tUhkVxDoX685KJSXfgg1"
	if !Check(password, key) {
		t.Error("Password and salt should match the given key")
	}
}
