package tlogclient

import (
	"fmt"
	"io"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

func (c *Client) encodeHandshakeCapnp(firstSequence uint64, resetFirstSeq bool) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(c.capnpSegmentBuf))
	if err != nil {
		return fmt.Errorf("failed to build (handshake) capnp: %s", err.Error())
	}

	handshake, err := schema.NewRootHandshakeRequest(seg)
	if err != nil {
		return fmt.Errorf("couldn't create handshake: %s", err.Error())
	}

	handshake.SetVersion(zerodisk.CurrentVersion.UInt32())
	handshake.SetFirstSequence(firstSequence)
	handshake.SetResetFirstSequence(resetFirstSeq)

	err = handshake.SetVdiskID(c.vdiskID)
	if err != nil {
		return fmt.Errorf("couldn't set handshake vdiskID: %s", err.Error())
	}

	return capnp.NewEncoder(c.bw).Encode(msg)
}

func (c *Client) encodeBlockCapnp(w io.Writer, op uint8, seq uint64, index int64, hash []byte, timestamp int64, data []byte) (*schema.TlogBlock, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(c.capnpSegmentBuf))
	if err != nil {
		return nil, fmt.Errorf("failed to build build (block) capnp: %s", err.Error())
	}

	block, err := schema.NewRootTlogBlock(seg)
	if err != nil {
		return nil, fmt.Errorf("couldn't create block: %s", err.Error())
	}

	err = block.SetHash(hash)
	if err != nil {
		return nil, fmt.Errorf("couldn't set block hash: %s", err.Error())
	}
	err = block.SetData(data)
	if err != nil {
		return nil, fmt.Errorf("couldn't set block data: %s", err.Error())
	}

	block.SetOperation(op)
	block.SetSequence(seq)
	block.SetIndex(index)
	block.SetTimestamp(timestamp)

	return &block, capnp.NewEncoder(w).Encode(msg)
}

// encode and send command
func (c *Client) encodeSendCommand(w io.Writer, cmdType uint8, seq uint64) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return err
	}
	cmd, err := schema.NewRootCommand(seg)
	if err != nil {
		return err
	}
	cmd.SetType(cmdType)
	cmd.SetSequence(seq)

	return capnp.NewEncoder(w).Encode(msg)
}

func (c *Client) decodeHandshakeResponse() (*schema.HandshakeResponse, error) {
	msg, err := capnp.NewDecoder(c.conn).Decode()
	if err != nil {
		return nil, err
	}

	resp, err := schema.ReadRootHandshakeResponse(msg)
	return &resp, err
}

func (c *Client) decodeBlockResponse(rd io.Reader) (*schema.TlogResponse, error) {
	msg, err := capnp.NewDecoder(rd).Decode()
	if err != nil {
		return nil, err
	}

	tr, err := schema.ReadRootTlogResponse(msg)
	return &tr, err
}
