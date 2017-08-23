package meta

import (
	"crypto/rand"
	mr "math/rand"
	"net"
	"testing"
	"time"

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

// test that client can return gracefully when the server is not exist
func TestServerNotExist(t *testing.T) {
	_, err := NewClient([]string{"http://localhost:1234"})

	// make sure it is network error
	_, ok := err.(net.Error)
	require.True(t, ok)
}

// test that client can return gracefully when the server is down
func TestServerDown(t *testing.T) {
	etcd, err := embedserver.New()
	require.Nil(t, err)

	c, err := NewClient([]string{etcd.ListenAddr()})
	require.Nil(t, err)

	key := "key"
	md, err := New([]byte(key), 0, []string{"12345"})
	require.Nil(t, err)

	// make sure we can do some operation to server
	err = c.Put(key, md)
	require.Nil(t, err)

	_, err = c.Get(key)
	require.Nil(t, err)

	// stop the server
	etcd.Stop()

	// test the PUT
	done := make(chan struct{}, 1)
	go func() {
		err = c.Put(key, md)
		_, ok := err.(net.Error)
		require.True(t, ok)
		done <- struct{}{}
	}()

	select {
	case <-time.After(metaOpTimeout + (5 * time.Second)):
		// the put operation should be exited before the timeout
		t.Fatal("the operation should already returns with error")
	case <-done:
		t.Logf("operation exited successfully")
	}

	// test the GET
	done = make(chan struct{}, 1)
	go func() {
		_, err = c.Get(key)
		_, ok := err.(net.Error)
		require.True(t, ok)
		done <- struct{}{}
	}()

	select {
	case <-time.After(metaOpTimeout + (5 * time.Second)):
		// the Get operation should be exited before the timeout
		t.Fatal("the operation should already returns with error")
	case <-done:
		t.Logf("operation exited successfully")
	}

}
