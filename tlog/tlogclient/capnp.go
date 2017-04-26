package tlogclient

import (
	"bytes"
	"fmt"

	"zombiezen.com/go/capnproto2"
)

const (
	volIDLen = 32
)

func decodeResponse(data []byte) (*TlogResponse, error) {
	msg, err := capnp.NewDecoder(bytes.NewBuffer(data)).Decode()
	if err != nil {
		return nil, err
	}

	tr, err := ReadRootTlogResponse(msg)
	return &tr, err
}

func buildCapnp(volID string, seq uint64, crc uint32,
	lba, timestamp uint64, data []byte) ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, fmt.Errorf("build capnp:%v", err)
	}

	block, err := NewRootTlogBlock(seg)
	if err != nil {
		return nil, fmt.Errorf("create block:%v", err)
	}

	// we pad the volume ID to get fixed length volume ID
	if len(volID) < volIDLen {
		pad := make([]byte, volIDLen-len(volID))
		b := append([]byte(volID), pad...)
		volID = string(b)
	}

	block.SetVolumeId(volID)
	block.SetSequence(seq)
	block.SetLba(lba)
	block.SetCrc32(crc)
	block.SetTimestamp(timestamp)
	block.SetSize(uint32(len(data)))
	block.SetData(data)

	buf := new(bytes.Buffer)

	err = capnp.NewEncoder(buf).Encode(msg)

	return buf.Bytes(), err
}
