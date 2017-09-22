package client

import (
	"fmt"
	"sync"

	"github.com/zero-os/0-stor/client/meta"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/manager"
)

// SetReferenceList replace the complete reference list for the object pointed by key
func (c *Client) SetReferenceList(key []byte, refList []string) error {
	md, err := c.metaCli.Get(string(key))
	if err != nil {
		return err
	}
	return c.SetReferenceListWithMeta(md, refList)
}

// SetReferenceListWithMeta is the same as SetReferenceList but take metadata instead of key
// as argument
func (c *Client) SetReferenceListWithMeta(md *meta.Meta, refList []string) error {
	if len(refList) > db.RefIDCount {
		return fmt.Errorf("too many reference list: %v, max: %v", len(refList), db.RefIDCount)
	}
	return c.updateRefListWithMeta(md, refList, manager.RefListOpSet)
}

// AppendReferenceList adds some reference to the reference list of the object pointed by key
func (c *Client) AppendReferenceList(key []byte, refList []string) error {
	md, err := c.metaCli.Get(string(key))
	if err != nil {
		return err
	}
	return c.AppendReferenceListWithMeta(md, refList)
}

// AppendReferenceListWithMeta is the same as AppendReferenceList but take metadata instead of key
// as argument
func (c *Client) AppendReferenceListWithMeta(md *meta.Meta, refList []string) error {
	if len(refList) > db.RefIDCount {
		return fmt.Errorf("too many reference list: %v, max: %v", len(refList), db.RefIDCount)
	}
	return c.updateRefListWithMeta(md, refList, manager.RefListOpAppend)
}

// RemoveReferenceList removes some reference from the reference list of the object pointed by key
func (c *Client) RemoveReferenceList(key []byte, refList []string) error {
	md, err := c.metaCli.Get(string(key))
	if err != nil {
		return err
	}
	return c.RemoveReferenceListWithMeta(md, refList)
}

// RemoveReferenceListWithMeta is the same as RemoveReferenceList but take metadata
// instead of key as argument
func (c *Client) RemoveReferenceListWithMeta(md *meta.Meta, refList []string) error {
	if len(refList) > db.RefIDCount {
		return fmt.Errorf("too many reference list: %v, max: %v", len(refList), db.RefIDCount)
	}
	return c.updateRefListWithMeta(md, refList, manager.RefListOpRemove)
}

func (c *Client) updateRefListWithMeta(md *meta.Meta, refList []string, op int) error {
	for _, chunk := range md.Chunks {

		var (
			wg    sync.WaitGroup
			errCh = make(chan error, len(chunk.Shards))
		)

		wg.Add(len(chunk.Shards))
		for _, shard := range chunk.Shards {
			go func(shard string) {
				defer wg.Done()

				// get stor client
				storCli, err := c.getStor(shard)
				if err != nil {
					errCh <- err
					return
				}

				// do the work
				switch op {
				case manager.RefListOpSet:
					err = storCli.ReferenceSet(chunk.Key, refList)
				case manager.RefListOpAppend:
					err = storCli.ReferenceAppend(chunk.Key, refList)
				case manager.RefListOpRemove:
					err = storCli.ReferenceRemove(chunk.Key, refList)
				default:
					err = fmt.Errorf("wrong operation: %v", op)
				}
				if err != nil {
					errCh <- err
					return
				}
			}(shard)
		}

		wg.Wait()

		if len(errCh) > 0 {
			err := <-errCh
			return err
		}
	}
	return nil
}
