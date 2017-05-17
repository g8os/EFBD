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

// Tlog Response status values
const (
	BlockStatusFlushFailed BlockStatus = -2
	BlockStatusRecvFailed  BlockStatus = -1
	BlockStatusRecvOK      BlockStatus = 1
	BlockStatusFlushOK     BlockStatus = 2
)

// VerAckStatus is returned by the Tlog server
// when receiving the initial VerAck message
// from the client
type VerAckStatus int8

// Int8 returns the verAck status as an int8 value
func (status VerAckStatus) Int8() int8 {
	return int8(status)
}

// Error returns this status as an error,
// returning nil if the status is not an error
func (status VerAckStatus) Error() error {
	if status > 0 {
		return nil
	}

	switch status {
	case VerackStatusInvalidVersion:
		return errors.New("client version is not compatible with server")
	default:
		return fmt.Errorf("invalid connection with unknown status: %d", status)
	}
}

// VerAck status values
const (
	// returned when the given version by the client
	// was not supported by the server
	VerackStatusInvalidVersion VerAckStatus = -1
	// returned when all information was accepted
	VerackStatusOK VerAckStatus = 1
)
