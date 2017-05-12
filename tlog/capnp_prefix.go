package tlog

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	capnpWordLen = 8
)

// WriteCapnpPrefix writes capnp prefix used in stream message.
// As described at https://capnproto.org/encoding.html#serialization-over-a-stream
func WriteCapnpPrefix(w io.Writer, length int) error {
	var prefix uint32

	if length%capnpWordLen != 0 {
		return fmt.Errorf("capnp prefix : length should be aligned to: %v. length=%v", capnpWordLen, length)
	}

	if err := binary.Write(w, binary.LittleEndian, prefix); err != nil { // write zero
		return err
	}

	prefix = uint32(length / capnpWordLen)

	return binary.Write(w, binary.LittleEndian, prefix)
}

// ReadCapnpPrefix reads capnp prefix used in stream message.
// As described at https://capnproto.org/encoding.html#serialization-over-a-stream
func ReadCapnpPrefix(rd io.Reader) (segmentNum, length uint32, err error) {
	if err = binary.Read(rd, binary.LittleEndian, &segmentNum); err != nil {
		return
	}
	if err = binary.Read(rd, binary.LittleEndian, &length); err != nil {
		return
	}

	length = length * capnpWordLen
	return
}
