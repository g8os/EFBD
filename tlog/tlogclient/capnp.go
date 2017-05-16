package tlogclient

import (
	"fmt"

	"github.com/g8os/blockstor/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

func (c *Client) decodeResponse() (*schema.TlogResponse, error) {
	msg, err := capnp.NewDecoder(c.conn).Decode()
	if err != nil {
		return nil, err
	}

	tr, err := schema.ReadRootTlogResponse(msg)
	return &tr, err
}

func (c *Client) encodeCapnp(op uint8, seq uint64, hash []byte,
	lba, timestamp uint64, data []byte, size uint64) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(c.capnpSegmentBuf))
	if err != nil {
		return fmt.Errorf("build capnp:%v", err)
	}

	block, err := schema.NewRootTlogBlock(seg)
	if err != nil {
		return fmt.Errorf("couldn't create block:%v", err)
	}

	block.SetOperation(op)
	block.SetSequence(seq)
	block.SetLba(lba)
	block.SetHash(hash)
	block.SetTimestamp(timestamp)
	block.SetSize(size)
	block.SetData(data)

	clientPackage, err := schema.NewRootTlogClientPackage(seg)
	if err != nil {
		return fmt.Errorf("couldn't create package: %v", err)
	}
	clientPackage.SetVdiskID(c.vdiskID)
	clientPackage.SetBlock(block)

	return capnp.NewEncoder(c.bw).Encode(msg)
}
