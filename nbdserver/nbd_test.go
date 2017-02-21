package main

import (
	"testing"

	"github.com/abligh/gonbdserver/nbd"
)

func TestRegisteredBackend(t *testing.T) {
	// bcreator, ok := nbd.BackendMap["ardb"]
	_, ok := nbd.BackendMap["ardb"]
	if !ok {
		t.Error("No 'ardb' backend registered")
		return
	}
	// b, err := bcreator(nil, nil)
	// if err != nil {
	// 	t.Error("Error creating ardbackend:", err)
	// 	return
	// }
	// _, ok = b.(*ArdbBackend)
	// if !ok {
	// 	t.Error("No ardbbackend returned by registerd creation function")
	// 	return
	// }
}
