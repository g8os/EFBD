package stor

import (
	"bytes"
	"time"

	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk/tlog/schema"
)

func (c *Client) encodeCapnp(blocks []*schema.TlogBlock) ([]byte, error) {
	// create the aggregation object
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(c.capnpBuf))
	if err != nil {
		return nil, err
	}
	agg, err := schema.NewRootTlogAggregation(seg)
	if err != nil {
		return nil, err
	}

	agg.SetSize(uint64(len(blocks)))
	agg.SetTimestamp(uint64(time.Now().UnixNano()))
	agg.SetVdiskID(c.vdiskID)

	// insert the blocks to the aggregation
	blockList, err := agg.NewBlocks(int32(len(blocks)))
	if err != nil {
		return nil, err
	}

	for i := 0; i < blockList.Len(); i++ {
		block := blockList.At(i)
		if err := schema.CopyBlock(&block, blocks[i]); err != nil {
			return nil, err
		}
	}

	// encode it
	buf := new(bytes.Buffer)
	err = capnp.NewEncoder(buf).Encode(msg)
	return buf.Bytes(), err
}

func (c *Client) decodeCapnp(data []byte) (*schema.TlogAggregation, error) {
	buf := bytes.NewBuffer(data)

	msg, err := capnp.NewDecoder(buf).Decode()
	if err != nil {
		return nil, err
	}

	agg, err := schema.ReadRootTlogAggregation(msg)
	return &agg, err
}
