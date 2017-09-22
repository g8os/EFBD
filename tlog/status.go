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

// String returns the name of the block status
func (status BlockStatus) String() string {
	switch status {
	case BlockStatusRecvOK:
		return "RecvOK"
	case BlockStatusFlushOK:
		return "FlushOK"
	case BlockStatusFlushFailed:
		return "FlushFailed"
	case BlockStatusRecvFailed:
		return "RecvFailed"
	case BlockStatusForceFlushReceived:
		return "ForceFlushCommandReceived"
	default:
		return "Unknown"
	}
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
	BlockStatusFlushFailed              BlockStatus = -2
	BlockStatusRecvFailed               BlockStatus = -1
	BlockStatusRecvOK                   BlockStatus = 1
	BlockStatusFlushOK                  BlockStatus = 2
	BlockStatusForceFlushReceived       BlockStatus = 3
	BlockStatusWaitNbdSlaveSyncReceived BlockStatus = 4
	BlockStatusDisconnected             BlockStatus = 5
)

// HandshakeStatus is returned by the Tlog server
// when receiving the initial handshake message
// from the client
type HandshakeStatus int8

// Int8 returns the Handshake status as an int8 value
func (status HandshakeStatus) Int8() int8 {
	return int8(status)
}

// String returns the name of the Hanshake status
func (status HandshakeStatus) String() string {
	switch status {
	case HandshakeStatusOK:
		return "OK"
	case HandshakeStatusInvalidVersion:
		return "InvalidVersion"
	case HandshakeStatusInvalidVdiskID:
		return "InvalidVdiskID"
	case HandshakeStatusInternalServerError:
		return "InternalServerError"
	case HandshakeStatusInvalidRequest:
		return "InvalidRequest"
	default:
		return "Unknown"
	}
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
	case HandshakeStatusInternalServerError:
		return errors.New("internal server error")
	case HandshakeStatusAttachFailed:
		return errors.New("failed to attach to vdisk")
	case HandshakeStatusInvalidVersion:
		return errors.New("client version is not compatible with server")
	case HandshakeStatusInvalidRequest:
		return errors.New("client's HandshakeRequest could not be decoded")
	default:
		return fmt.Errorf("invalid connection with unknown status: %d", status)
	}
}

// Handshake status values
const (
	// returned when the connection failed to attach to vdisk.
	// it currently only happened when a connection trying to
	// to connect to an already used vdisk
	HandshakeStatusAttachFailed HandshakeStatus = -5
	// returned when something went wrong internally
	// in the tlogserver during the handshake phase
	HandshakeStatusInternalServerError HandshakeStatus = -4
	// returned when the given VdiskID is not legal
	// could be because it's not valid, or because it already
	// exists on the server
	HandshakeStatusInvalidVdiskID HandshakeStatus = -3
	// returned when the given version by the client
	// was not supported by the server
	HandshakeStatusInvalidVersion HandshakeStatus = -2
	// returned when the given request by the client
	// could not be read
	HandshakeStatusInvalidRequest HandshakeStatus = -1
	// returned when all information was accepted
	HandshakeStatusOK HandshakeStatus = 1
)
