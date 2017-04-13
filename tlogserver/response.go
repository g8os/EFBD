package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/g8os/tlog/client"
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

func (r *response) toCapnp() (*capnp.Message, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}

	resp, err := client.NewTlogResponse(seg)
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

func (r *response) write(w io.Writer) error {
	msg, err := r.toCapnp()
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)

	if err := capnp.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	prefix := fmt.Sprintf("%v\r\n", buf.Len())

	w.Write([]byte(prefix))

	_, err = buf.WriteTo(w)
	return err
}
