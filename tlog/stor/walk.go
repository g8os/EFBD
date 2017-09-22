package stor

import (
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-stor/client/meta"
)

// WalkResult represents result of Walk method
type WalkResult struct {
	// Tlog aggregation
	Agg *schema.TlogAggregation

	// Key in 0-stor server
	StorKey []byte

	// 0-stor metadata
	Meta *meta.Meta

	// Raw data
	Data []byte

	// Reference list
	RefList []string

	// Error
	Err error
}

// Walk walks the history from fromEpoch to toEpoch.
func (c *Client) Walk(fromEpoch, toEpoch int64) <-chan *WalkResult {

	wrCh := make(chan *WalkResult, 2)
	go func() {
		defer close(wrCh)

		if len(c.firstMetaKey) == 0 {
			// we have no data yet
			return
		}

		for res := range c.storClient.Walk([]byte(c.firstMetaKey), fromEpoch, toEpoch) {
			wr := &WalkResult{
				StorKey: res.Key,
				Meta:    res.Meta,
				Data:    res.Data,
				RefList: res.RefList,
			}

			// make sure it is not error
			if res.Error != nil {
				wr.Err = res.Error
				wrCh <- wr
				return
			}

			// decode capnp
			agg, err := c.decodeCapnp(res.Data)
			if err != nil {
				wr.Err = err
				wrCh <- wr
				return
			}

			wr.Agg = agg
			wrCh <- wr
		}
	}()
	return wrCh
}
