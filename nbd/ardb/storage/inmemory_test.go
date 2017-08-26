package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryStorage(t *testing.T) {
	const (
		vdiskID   = "a"
		blockSize = 8
	)

	blockStorage := NewInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, blockStorage) {
		return
	}

	testBlockStorage(t, blockStorage)
}

func TestInMemoryStorageForceFlush(t *testing.T) {
	const (
		vdiskID   = "a"
		blockSize = 8
	)

	blockStorage := NewInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, blockStorage) {
		return
	}

	testBlockStorageForceFlush(t, blockStorage)
}
