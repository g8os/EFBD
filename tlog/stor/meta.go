package stor

import (
	"github.com/zero-os/0-stor/client/meta"
)

func (c *Client) getLastMetaKey() (metaKey []byte, md *meta.Meta, err error) {
	md, err = c.storClient.GetMeta(c.lastMetaEtcdKey)
	if err != nil {
		return
	}

	metaKey, err = md.Key()
	if err != nil {
		return
	}

	md, err = c.storClient.GetMeta(metaKey)
	return
}
func (c *Client) saveLastMetaKey(key []byte) error {
	md, err := meta.New(key, 0, nil)
	if err != nil {
		return err
	}

	return c.storClient.PutMeta(c.lastMetaEtcdKey, md)
}
