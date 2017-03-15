package lba

import (
	"crypto/rand"
	"testing"
)

func TestHashBytes(t *testing.T) {
	data := make([]byte, 435)
	rand.Read(data)
	h := HashBytes(data)
	if h == nil {
		t.Error("Nil hash returned from the hashfunction")
		return
	}
	if h.Equals(NilHash) {
		t.Error("Empty hash returned")
	}
}
