package server

import (
	"io"

	"zombiezen.com/go/capnproto2"

	"github.com/g8os/blockstor/tlog/schema"
)

type response struct {
	Status    int8
	Sequences []uint64
}

func (r *response) toCapnp(segmentBuf []byte) (*capnp.Message, error) {
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

func (r *response) write(w io.Writer, segmentBuf []byte) error {
	msg, err := r.toCapnp(segmentBuf)
	if err != nil {
		return err
	}

	return capnp.NewEncoder(w).Encode(msg)
}
