package stor

import (
	"bytes"

	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk/tlog/schema"
)

func (c *Client) decodeCapnp(data []byte) (*schema.TlogAggregation, error) {
	buf := bytes.NewBuffer(data)

	msg, err := capnp.NewDecoder(buf).Decode()
	if err != nil {
		return nil, err
	}

	agg, err := schema.ReadRootTlogAggregation(msg)
	return &agg, err
}
