package tlogclient

import (
	"io"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/tlog/schema"
	"zombiezen.com/go/capnproto2"
)

func (c *Client) encodeHandshakeCapnp() error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(c.capnpSegmentBuf))
	if err != nil {
		return errors.Wrap(err, "failed to build (handshake) capnp")
	}

	handshake, err := schema.NewRootHandshakeRequest(seg)
	if err != nil {
		return errors.Wrap(err, "couldn't create handshake")
	}

	handshake.SetVersion(zerodisk.CurrentVersion.UInt32())

	err = handshake.SetVdiskID(c.vdiskID)
	if err != nil {
		return errors.Wrap(err, "couldn't set handshake vdiskID")
	}

	return capnp.NewEncoder(c.bw).Encode(msg)
}

func encodeBlockCapnp(w io.Writer, op uint8, seq uint64, index int64, hash []byte, timestamp int64, data, segmentBuf []byte) (*schema.TlogBlock, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(segmentBuf))
	if err != nil {
		return nil, errors.Wrap(err, "failed to build build (block) capnp")
	}

	cmd, err := schema.NewRootTlogClientMessage(seg)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create client message")
	}

	block, err := schema.NewTlogBlock(seg)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create block")
	}

	err = block.SetHash(hash)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't set block hash")
	}
	err = block.SetData(data)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't set block data")
	}

	block.SetOperation(op)
	block.SetSequence(seq)
	block.SetIndex(index)
	block.SetTimestamp(timestamp)

	cmd.SetBlock(block)

	return &block, capnp.NewEncoder(w).Encode(msg)
}

// encode and send ForceFlushAtSeq command
func encodeForceFlushAtSeq(w io.Writer, seq uint64) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return err
	}
	cmd, err := schema.NewRootTlogClientMessage(seg)
	if err != nil {
		return err
	}
	cmd.SetForceFlushAtSeq(seq)

	return capnp.NewEncoder(w).Encode(msg)
}

// encode and send WaitNBDSlaveSync command
func encodeWaitNBDSlaveSync(w io.Writer) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return err
	}
	cmd, err := schema.NewRootTlogClientMessage(seg)
	if err != nil {
		return err
	}
	cmd.SetWaitNBDSlaveSync()

	return capnp.NewEncoder(w).Encode(msg)
}

func encodeDisconnect(w io.Writer) error {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return err
	}
	cmd, err := schema.NewRootTlogClientMessage(seg)
	if err != nil {
		return err
	}
	cmd.SetDisconnect()

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

func (c *Client) decodeTlogResponse(rd io.Reader) (*schema.TlogResponse, error) {
	msg, err := capnp.NewDecoder(rd).Decode()
	if err != nil {
		return nil, err
	}

	tr, err := schema.ReadRootTlogResponse(msg)
	return &tr, err
}
