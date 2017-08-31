package meta

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-stor/client/meta/embedserver"
)

func TestRoundTrip(t *testing.T) {
	etcd, err := embedserver.New()
	require.NoError(t, err)
	defer etcd.Stop()

	c, err := NewClient([]string{etcd.ListenAddr()})
	require.NoError(t, err)

	// prepare the data
	md := createCapnpMeta(t)

	// put the metadata
	err = c.Put(string(md.Key), md)
	require.NoError(t, err)

	// get it back
	storedMd, err := c.Get(string(md.Key))
	require.NoError(t, err)

	// check stored value
	assert.Equal(t, md.Key, storedMd.Key, "key is different")
	assert.Equal(t, md.Size(), storedMd.Size(), "size is different")
	assert.Equal(t, md.Epoch, storedMd.Epoch, "epoch is different")
	assert.Equal(t, md.EncrKey, storedMd.EncrKey, "encryption key is different")
	assert.Equal(t, md.Previous, storedMd.Previous, "previous pointer is different")
	assert.Equal(t, md.Next, storedMd.Next, "next pointer is different")
	assert.Equal(t, md.ConfigPtr, storedMd.ConfigPtr, "config pointer is different")
	assert.Equal(t, md.Chunks, storedMd.Chunks, "chunks are differents")
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
	md := New([]byte(key))

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
		t.Error("the operation should already returns with error")
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
		t.Error("the operation should already returns with error")
	case <-done:
		t.Logf("operation exited successfully")
	}

}
