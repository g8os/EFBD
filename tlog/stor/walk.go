package stor

import (
	"github.com/zero-os/0-Disk/tlog/schema"
)

type WalkResult struct {
	Agg *schema.TlogAggregation
	Err error
}

func (c *Client) Walk(fromEpoch, toEpoch uint64) <-chan *WalkResult {

	wrCh := make(chan *WalkResult, 2)
	go func() {
		defer close(wrCh)

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

			// check the epoch
			epoch := agg.Timestamp()
			if epoch < fromEpoch {
				// still long way to go, proceed to next key
				continue
			}
			if epoch > toEpoch {
				// we passed it
				return
			}

			wr.Agg = agg
			wrCh <- wr
			return
		}
	}()
	return wrCh
}
