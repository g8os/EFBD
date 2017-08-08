package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

func TestInMemoryBackendReadWrite(t *testing.T) {
	const (
		vdiskID   = "a"
		size      = 64
		blockSize = 8
	)

	storage := storage.NewInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	ctx := context.Background()

	testBackendReadWrite(ctx, t, vdiskID, blockSize, size, storage)
}
