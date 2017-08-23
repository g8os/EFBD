package client

import (
	"errors"

	"github.com/zero-os/0-stor/client/meta"
)

var (
	errNilMetaClient = errors.New("nil meta client : please specify meta server adddress during client creation")
)

// PutMeta puts metadata to metadata server
func (c *Client) PutMeta(key []byte, md *meta.Meta) error {
	if c.metaCli == nil {
		return errNilMetaClient
	}
	return c.metaCli.Put(string(key), md)
}

// GetMeta gets metadata from metadata server
func (c *Client) GetMeta(key []byte) (*meta.Meta, error) {
	if c.metaCli == nil {
		return nil, errNilMetaClient
	}
	return c.metaCli.Get(string(key))
}
