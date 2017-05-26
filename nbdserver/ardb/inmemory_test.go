package ardb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryStorage(t *testing.T) {
	const (
		vdiskID   = "a"
		blockSize = 8
	)

	storage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	testBackendStorage(t, storage)
}

func TestInMemoryStorageDeadlock(t *testing.T) {
	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	storage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	testBackendStorageDeadlock(t, blockSize, blockCount, storage)
}
