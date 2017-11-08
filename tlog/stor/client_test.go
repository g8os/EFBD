package stor

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/meta/embedserver"
	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
)

func TestRoundTrip(t *testing.T) {
	const (
		vdiskID      = "12345678"
		numData      = 10
		dataShards   = 4
		parityShards = 2
	)

	mdServer, err := embedserver.New()
	require.Nil(t, err)
	defer mdServer.Stop()

	storCluster, err := embeddedserver.NewZeroStorCluster(dataShards + parityShards)
	require.Nil(t, err)
	defer storCluster.Close()

	cli := createTestClient(t, vdiskID, dataShards, parityShards, mdServer.ListenAddr(),
		storCluster.Addrs())

	// send the data
	var vals [][]byte
	for i := 0; i < numData; i++ {
		val := make([]byte, 1024)
		rand.Read(val)

		block := encodeBlock(t, val)

		_, err := cli.ProcessStore([]*schema.TlogBlock{block})
		require.Nil(t, err)

		vals = append(vals, val)
	}

	// walk over it
	var i int
	for wr := range cli.Walk(0, tlog.TimeNowTimestamp()) {
		require.Nil(t, wr.Err)
		agg := wr.Agg
		blocks, err := agg.Blocks()
		require.Nil(t, err)

		require.Equal(t, 1, blocks.Len())
		block := blocks.At(0)
		data, err := block.Data()
		require.Nil(t, err)

		require.Equal(t, vals[i], data)
		i++
	}
	require.Equal(t, numData, i)
}

func encodeBlock(t *testing.T, data []byte) *schema.TlogBlock {
	buf := make([]byte, 0, 4096)
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(buf))
	require.Nil(t, err)

	block, err := schema.NewRootTlogBlock(seg)
	require.Nil(t, err)

	err = block.SetData(data)
	require.Nil(t, err)

	return &block
}

func createTestClient(t *testing.T, vdiskID string, dataShards, parityShards int,
	mdServerAddr string, storClusterAddrs []string) *Client {

	conf := Config{
		VdiskID:         vdiskID,
		Organization:    "testorg",
		Namespace:       "thedisk",
		IyoClientID:     "",
		IyoSecret:       "",
		ZeroStorShards:  storClusterAddrs,
		MetaShards:      []string{mdServerAddr},
		DataShardsNum:   dataShards,
		ParityShardsNum: parityShards,
		EncryptPrivKey:  "12345678901234567890123456789012",
	}
	cli, err := NewClient(conf)
	require.Nil(t, err)

	return cli
}
