package ardb

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
)

func TestMapErrorToBroadcastStatus(t *testing.T) {
	assert := assert.New(t)

	// unknown errors return false
	_, ok := MapErrorToBroadcastStatus(nil)
	assert.False(ok)
	_, ok = MapErrorToBroadcastStatus(errors.New("foo"))
	assert.False(ok)

	// all possible sucesfull map scenarios:

	// map EOF errors
	status, ok := MapErrorToBroadcastStatus(io.EOF)
	if assert.True(ok) {
		assert.Equal(status, log.StatusServerDisconnect)
	}

	// map net.Errors
	status, ok = MapErrorToBroadcastStatus(stubNetError{false, false})
	if assert.True(ok) {
		assert.Equal(status, log.StatusUnknownError)
	}
	status, ok = MapErrorToBroadcastStatus(stubNetError{false, true})
	if assert.True(ok) {
		assert.Equal(status, log.StatusServerTempError)
	}
	status, ok = MapErrorToBroadcastStatus(stubNetError{true, false})
	if assert.True(ok) {
		assert.Equal(status, log.StatusServerTimeout)
	}
	status, ok = MapErrorToBroadcastStatus(stubNetError{true, true})
	if assert.True(ok) {
		assert.Equal(status, log.StatusServerTimeout)
	}
}

type stubNetError struct {
	timeout, temporary bool
}

func (err stubNetError) Timeout() bool {
	return err.timeout
}

func (err stubNetError) Temporary() bool {
	return err.temporary
}

func (err stubNetError) Error() string {
	return "stub net error"
}
