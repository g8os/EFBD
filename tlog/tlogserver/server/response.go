package server

import (
	"io"

	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk/tlog/schema"
)

// BlockResponse defines response for block operations(recv, flush)
type BlockResponse struct {
	Status    int8
	Sequences []uint64
}

func (r *BlockResponse) toCapnp(segmentBuf []byte) (*capnp.Message, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(segmentBuf))
	if err != nil {
		return nil, err
	}

	resp, err := schema.NewRootTlogResponse(seg)
	if err != nil {
		return nil, err
	}

	seqs, err := resp.NewSequences(int32(len(r.Sequences)))
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(r.Sequences); i++ {
		seqs.Set(i, r.Sequences[i])
	}

	resp.SetStatus(r.Status)
	resp.SetSequences(seqs)

	return msg, nil
}

// Write encode and writes this response to the given io.Writer
func (r *BlockResponse) Write(w io.Writer, segmentBuf []byte) error {
	msg, err := r.toCapnp(segmentBuf)
	if err != nil {
		return err
	}

	return capnp.NewEncoder(w).Encode(msg)
}
