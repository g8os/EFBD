package meta

import (
	"crypto/rand"
	mr "math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-stor/client/meta/embedserver"
)

func TestRoundTrip(t *testing.T) {
	etcd, err := embedserver.New()
	require.Nil(t, err)
	defer etcd.Stop()

	c, err := NewClient([]string{etcd.ListenAddr()})
	require.Nil(t, err)

	// prepare the data
	key := make([]byte, 32)
	rand.Read(key)

	size := mr.Uint64()
	shards := []string{"http://1.2.3.5:1234"}

	// create the metadata object
	md, err := New(key, size, shards)
	require.Nil(t, err)

	// put the metadata
	err = c.Put(string(key), md)
	require.Nil(t, err)

	// get it back
	storedMd, err := c.Get(string(key))
	require.Nil(t, err)

	// check stored value
	storedKey, err := storedMd.Key()
	require.Nil(t, err)
	require.Equal(t, key, storedKey)

	require.Equal(t, size, storedMd.Size())

	storedShards, err := md.GetShardsSlice()
	require.Nil(t, err)
	require.Equal(t, shards, storedShards)
}
