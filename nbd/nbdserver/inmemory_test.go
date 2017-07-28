package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryStorage(t *testing.T) {
	const (
		vdiskID   = "a"
		blockSize = 8
	)

	blockStorage := newInMemoryStorage(vdiskID, blockSize)
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

	blockStorage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, blockStorage) {
		return
	}

	testBlockStorageForceFlush(t, blockStorage)
}

func TestInMemoryBackendReadWrite(t *testing.T) {
	const (
		vdiskID   = "a"
		size      = 64
		blockSize = 8
	)

	storage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	ctx := context.Background()

	testBackendReadWrite(ctx, t, vdiskID, blockSize, size, storage)
}
