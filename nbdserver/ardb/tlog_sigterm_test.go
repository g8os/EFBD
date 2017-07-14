package ardb

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTlogSigtermHandler(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// create tlogStorage
	const (
		vdiskID   = "a"
		blockSize = 8
	)

	storage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	tlogrpc := newTlogTestServer(ctx, t)
	if !assert.NotEmpty(t, tlogrpc) {
		return
	}

	vComp := &vdiskCompletion{}
	tls, err := newTlogStorage(vdiskID, tlogrpc, "", blockSize, storage, vComp)
	if !assert.NoError(t, err) || !assert.NotNil(t, tls) {
		return
	}
	go tls.GoBackground(ctx)

	tls.Set(0, []byte{1, 2, 3, 4})
	tls.Set(1, []byte{1, 2, 3, 4})

	// sending SIGTERM
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	// make sure we get completion error object
	errs := vComp.Wait()
	assert.Equal(t, 1, len(errs))
	assert.Nil(t, errs[0])
}
