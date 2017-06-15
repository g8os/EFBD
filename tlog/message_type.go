package tlog

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Type of message sent from client to server
const (
	messageTypeStart        = iota
	MessageTlogBlock        // tlog block
	MessageForceFlushAtSeq  // force flush at sequence
	MessageWaitNbdSlaveSync // wait for nbd slave to be fully synced
	messageTypeEnd
)

// check validity of the message type
func checkMessageType(val uint8) error {
	if val <= messageTypeStart || val >= messageTypeEnd {
		return fmt.Errorf("invalid message type: %v", val)
	}
	return nil
}

// WriteMessageType writes type of message to given writer
func WriteMessageType(w io.Writer, mType uint8) error {
	if err := checkMessageType(mType); err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, mType)
}

// ReadCheckMessageType reads type of message from given reader
// and check the validity of the type
func ReadCheckMessageType(rd io.Reader) (uint8, error) {
	var mType uint8

	if err := binary.Read(rd, binary.LittleEndian, &mType); err != nil {
		return 0, err
	}

	return mType, checkMessageType(mType)
}
