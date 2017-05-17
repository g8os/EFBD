package tlogclient

import (
	"fmt"

	"github.com/g8os/blockstor"
	"github.com/g8os/blockstor/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

func (c *Client) encodeVerackCapnp() error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(c.capnpSegmentBuf))
	if err != nil {
		return fmt.Errorf("failed to build (verack) capnp: %s", err.Error())
	}

	verack, err := schema.NewRootClientVerAck(seg)
	if err != nil {
		return fmt.Errorf("couldn't create verack: %s", err.Error())
	}

	verack.SetVersion(blockstor.CurrentVersion.UInt32())

	err = verack.SetVdiskID(c.vdiskID)
	if err != nil {
		return fmt.Errorf("couldn't set verack vdiskID: %s", err.Error())
	}

	return capnp.NewEncoder(c.bw).Encode(msg)
}

func (c *Client) encodeBlockCapnp(op uint8, seq uint64, hash []byte,
	lba, timestamp uint64, data []byte, size uint64) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(c.capnpSegmentBuf))
	if err != nil {
		return fmt.Errorf("failed to build build (block) capnp: %s", err.Error())
	}

	block, err := schema.NewRootTlogBlock(seg)
	if err != nil {
		return fmt.Errorf("couldn't create block: %s", err.Error())
	}

	err = block.SetHash(hash)
	if err != nil {
		return fmt.Errorf("couldn't set block hash: %s", err.Error())
	}
	err = block.SetData(data)
	if err != nil {
		return fmt.Errorf("couldn't set block data: %s", err.Error())
	}

	block.SetOperation(op)
	block.SetSequence(seq)
	block.SetLba(lba)
	block.SetTimestamp(timestamp)
	block.SetSize(size)

	return capnp.NewEncoder(c.bw).Encode(msg)
}

func (c *Client) decodeVerackResponse() (*schema.ServerVerAck, error) {
	msg, err := capnp.NewDecoder(c.conn).Decode()
	if err != nil {
		return nil, err
	}

	tr, err := schema.ReadRootServerVerAck(msg)
	return &tr, err
}

func (c *Client) decodeBlockResponse() (*schema.TlogResponse, error) {
	msg, err := capnp.NewDecoder(c.conn).Decode()
	if err != nil {
		return nil, err
	}

	tr, err := schema.ReadRootTlogResponse(msg)
	return &tr, err
}
