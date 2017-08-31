package stor

import (
	"github.com/zero-os/0-Disk/tlog/schema"
)

type WalkResult struct {
	Agg *schema.TlogAggregation
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
			wr := &WalkResult{}

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
