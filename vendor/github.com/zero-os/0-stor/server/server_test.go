package server

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	err := os.Setenv("STOR_TESTING", "true")
	if err != nil {
		fmt.Printf("error trying to set STOR_TESTING environment variable: %v", err)
		os.Exit(1)
	}

	defer func() {
		_ = os.Unsetenv("STOR_TESTING")
	}()

	os.Exit(m.Run())
}
