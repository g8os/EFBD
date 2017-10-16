package tlogclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	tlogServer "github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

// TestReconnect test client can connect again after getting disconnected
func TestReconnectFromSend(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdisk   = "12345"
		numLogs = 50
	)

	// create test server
	clean, configSource, _ := newZeroStorDefaultConfig(t, vdisk)
	defer clean()

	serv, err := tlogServer.NewServer(testConf, configSource)
	require.Nil(t, err)
	go serv.Listen(ctx)

	client, err := New([]string{serv.ListenAddr()}, vdisk)
	require.Nil(t, err)
	defer client.Close()

	data := make([]byte, 4096)

	// send tlog, and disconnect it few times in the middle to test re-connect ability.
	go func() {
		for i := 0; i < numLogs; i++ {
			x := uint64(i)
			if i%5 == 0 {
				client.conn.Close() // simulate closed connection by closing the socket
			}

			// send
			err := client.Send(schema.OpSet, x+1, int64(x), int64(x), data)
			require.Nil(t, err)
		}
	}()

	waitForBlockReceivedResponse(t, client, 1, uint64(numLogs))
}

func TestReconnectFromForceFlush(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdisk = "12345"
	)
	// test server
	clean, configSource, _ := newZeroStorDefaultConfig(t, vdisk)
	defer clean()

	s, err := tlogServer.NewServer(testConf, configSource)
	require.Nil(t, err)
	go s.Listen(ctx)

	// Create client
	client, err := New([]string{s.ListenAddr()}, vdisk)
	require.Nil(t, err)

	// Simulate closed connection
	client.conn.Close()

	// Do forceFlush, it should reconnect here
	err = client.ForceFlushAtSeq(uint64(1))
	require.Nil(t, err)

	respCh := client.Recv()

	// Wait for the response
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("TestReconnectFromRead failed : too long")
	case resp := <-respCh:
		require.Nil(t, resp.Err)
		require.NotNil(t, resp.Resp)
		require.Equal(t, resp.Resp.Status, tlog.BlockStatusForceFlushReceived)
	}
}
