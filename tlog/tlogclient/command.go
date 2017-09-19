package tlogclient

import (
	"io"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/tlog/schema"
)

type command interface {
	encodeSend(w io.Writer) (*schema.TlogBlock, error)
}

type cmdBlock struct {
	op         uint8
	seq        uint64
	index      int64
	timestamp  int64
	data       []byte
	segmentBuf []byte
}

func (cmd cmdBlock) encodeSend(w io.Writer) (*schema.TlogBlock, error) {
	hash := zerodisk.HashBytes(cmd.data)
	return encodeBlockCapnp(w, cmd.op, cmd.seq, cmd.index, hash[:],
		cmd.timestamp, cmd.data, cmd.segmentBuf)
}

type cmdForceFlushAtSeq struct {
	seq uint64
}

func (cmd cmdForceFlushAtSeq) encodeSend(w io.Writer) (*schema.TlogBlock, error) {
	return nil, encodeForceFlushAtSeq(w, cmd.seq)
}

type cmdWaitNbdSlaveSync struct {
}

func (cmd cmdWaitNbdSlaveSync) encodeSend(w io.Writer) (*schema.TlogBlock, error) {
	return nil, encodeWaitNBDSlaveSync(w)
}

type cmdDisconnect struct {
}

func (cmd cmdDisconnect) encodeSend(w io.Writer) (*schema.TlogBlock, error) {
	return nil, encodeDisconnect(w)
}
