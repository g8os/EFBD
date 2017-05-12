package server

import (
	"bytes"
	"io"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"

	"zombiezen.com/go/capnproto2"
)

type response struct {
	Status    int8
	Sequences []uint64
}

// count the encoded capnp size object
func (r *response) capnpSize() int {
	return 1 /* status */ + (len(r.Sequences) * 8) /* sequences */ + 30 /* capnp overhead */
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

	buf := new(bytes.Buffer)

	if err := capnp.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	if err := tlog.WriteCapnpPrefix(w, buf.Len()); err != nil {
		return err
	}

	_, err = buf.WriteTo(w)
	return err
}
