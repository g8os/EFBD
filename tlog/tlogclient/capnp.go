package tlogclient

import (
	"bytes"
	"fmt"

	"github.com/g8os/blockstor/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

func decodeResponse(data []byte) (*schema.TlogResponse, error) {
	msg, err := capnp.NewDecoder(bytes.NewBuffer(data)).Decode()
	if err != nil {
		return nil, err
	}

	tr, err := schema.ReadRootTlogResponse(msg)
	return &tr, err
}

func buildCapnp(vdiskID string, seq uint64, hash []byte,
	lba, timestamp uint64, data []byte) ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, fmt.Errorf("build capnp:%v", err)
	}

	block, err := schema.NewRootTlogBlock(seg)
	if err != nil {
		return nil, fmt.Errorf("create block:%v", err)
	}

	block.SetVdiskID(vdiskID)
	block.SetSequence(seq)
	block.SetLba(lba)
	block.SetHash(hash)
	block.SetTimestamp(timestamp)
	block.SetSize(uint32(len(data)))
	block.SetData(data)

	buf := new(bytes.Buffer)

	err = capnp.NewEncoder(buf).Encode(msg)

	return buf.Bytes(), err
}
