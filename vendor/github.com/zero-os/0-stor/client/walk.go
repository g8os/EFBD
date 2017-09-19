package client

import (
	"errors"

	"github.com/zero-os/0-stor/client/meta"
)

var (
	errEpochPassed = errors.New("epoch passed")
)

// WalkResult is result of the walk operation
type WalkResult struct {
	// The key in metadata server
	Key []byte

	// Metadata object
	Meta *meta.Meta

	// Raw data stored in 0-stor server
	Data []byte

	// Reference list
	RefList []string

	// Error object if exist.
	// If not nil, all other fields shouldn't be used
	Error error
}

// nextFunc is func to get `next` pointer of metadata
// in forward mode : next is the next key
// in backward mode : next is the previous key
type nextFunc func(md *meta.Meta) (key []byte, err error)

// func to check that the walk still meet the epoch criteria
// - if nextKey not nil and err nil, then we should walk to next key
// - if err is not nil, we should stop
type checkEpochFunc func(epoch, fromEpoch, toEpoch int64, md *meta.Meta) (nextKey []byte, err error)

// Walk walks over the metadata linked list in forward fashion and
// fetch the data stored in 0-stor as described by the metadata
func (c *Client) Walk(startKey []byte, fromEpoch, toEpoch int64) <-chan *WalkResult {
	next := func(md *meta.Meta) ([]byte, error) {
		return md.Next, nil
	}

	checkEpoch := func(epoch, fromEpoch, toEpoch int64, md *meta.Meta) (nextKey []byte, err error) {
		if epoch < fromEpoch {
			// still long way to go, proceed to next key
			nextKey, err := next(md)
			if err != nil {
				return nil, err
			}
			return nextKey, nil
		}

		if epoch > toEpoch {
			// we passed it
			return nil, errEpochPassed
		}
		return nil, nil
	}
	return c.walk(startKey, fromEpoch, toEpoch, next, checkEpoch)
}

// WalkBack is backward version of the Walk
func (c *Client) WalkBack(startKey []byte, fromEpoch, toEpoch int64) <-chan *WalkResult {
	next := func(md *meta.Meta) ([]byte, error) {
		return md.Previous, nil
	}

	checkEpoch := func(epoch, fromEpoch, toEpoch int64, md *meta.Meta) (nextKey []byte, err error) {
		if epoch > toEpoch {
			// still long way to go, proceed to next key
			nextKey, err := next(md)
			if err != nil {
				return nil, err
			}
			return nextKey, nil
		}

		if epoch < toEpoch {
			// we passed it
			return nil, errEpochPassed
		}
		return nil, nil
	}
	return c.walk(startKey, fromEpoch, toEpoch, next, checkEpoch)
}

func (c *Client) walk(startKey []byte, fromEpoch, toEpoch int64, next nextFunc,
	checkEpoch checkEpochFunc) <-chan *WalkResult {

	wrCh := make(chan *WalkResult, 1)
	key := startKey

	go func() {
		defer close(wrCh)

		for {
			// get the meta
			md, err := c.metaCli.Get(string(key))
			if err != nil {
				wrCh <- &WalkResult{
					Error: err,
				}
				return
			}

			wr := &WalkResult{
				Key:  key,
				Meta: md,
			}

			// check if this meta is what we want
			nextKey, err := checkEpoch(md.Epoch, fromEpoch, toEpoch, md)
			if err != nil {
				if err == errEpochPassed {
					return
				}

				wr.Error = err
				wrCh <- wr
				return
			}

			if len(nextKey) > 0 {
				key = nextKey
				continue
			}

			// get the object from 0-stor server
			data, refList, err := c.Read(key)
			if err != nil {
				wr.Error = err
				wrCh <- wr
				return
			}
			wr.Data = data
			wr.RefList = refList

			// go to next key
			nextKey, err = next(md)
			if err != nil {
				wr.Error = err
				wrCh <- wr
				return
			}
			wrCh <- wr

			// nextKey is nil, this is the end of
			// linked list
			if len(nextKey) == 0 {
				return
			}

			key = nextKey
		}
	}()
	return wrCh
}
