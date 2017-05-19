package tlog

import (
	"errors"
	"fmt"
)

// BlockStatus is returned by the Tlog server
// when received block
type BlockStatus int8

// Int8 returns the block status as an int8 value
func (status BlockStatus) Int8() int8 {
	return int8(status)
}

// Error returns this status as an error,
// returning nil if the status is not an error
func (status BlockStatus) Error() error {
	if status > 0 {
		return nil
	}

	switch status {
	case BlockStatusFlushFailed:
		return errors.New("couldn't flush the aggregation(s)")
	case BlockStatusRecvFailed:
		return errors.New("didn't receive the tlog block")
	default:
		return fmt.Errorf("invalid connection with unknown status: %d", status)
	}
}

// Tlog Response status values
const (
	BlockStatusFlushFailed BlockStatus = -2
	BlockStatusRecvFailed  BlockStatus = -1
	BlockStatusRecvOK      BlockStatus = 1
	BlockStatusFlushOK     BlockStatus = 2
)

// HandshakeStatus is returned by the Tlog server
// when receiving the initial handshake message
// from the client
type HandshakeStatus int8

// Int8 returns the Handshake status as an int8 value
func (status HandshakeStatus) Int8() int8 {
	return int8(status)
}

// Error returns this status as an error,
// returning nil if the status is not an error
func (status HandshakeStatus) Error() error {
	if status > 0 {
		return nil
	}

	switch status {
	case HandshakeStatusInvalidVdiskID:
		return errors.New("given vdisk is not accepted by the server")
	case HandshakeStatusInvalidVersion:
		return errors.New("client version is not compatible with server")
	default:
		return fmt.Errorf("invalid connection with unknown status: %d", status)
	}
}

// Handshake status values
const (
	// returned when the given VdiskID is not legal
	// could be because it's not valid, or because it already
	// exists on the server
	HandshakeStatusInvalidVdiskID HandshakeStatus = -2
	// returned when the given version by the client
	// was not supported by the server
	HandshakeStatusInvalidVersion HandshakeStatus = -1
	// returned when all information was accepted
	HandshakeStatusOK HandshakeStatus = 1
)
