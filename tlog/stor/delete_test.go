package stor

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/meta/embedserver"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
)

func TestStoreDelete(t *testing.T) {
	const (
		numData      = 25
		lenData      = 4096
		vdiskID      = "12345678"
		dataShards   = 4
		parityShards = 2
	)

	// object cluster server
	mdServer, err := embedserver.New()
	require.Nil(t, err, "failed to create etcd server")
	defer mdServer.Stop()

	storCluster, err := embeddedserver.NewZeroStorCluster(dataShards + parityShards)
	require.Nil(t, err, "failed to create 0-stor cluster")
	defer storCluster.Close()

	cli := createTestClient(t, vdiskID, dataShards, parityShards, mdServer.ListenAddr(),
		storCluster.Addrs())

	// store the data
	log.Info("generates tlog data")
	for i := 0; i < numData; i++ {
		data := make([]byte, lenData)
		rand.Read(data)
		agg, err := tlog.NewAggregation(nil, 1)
		require.NoError(t, err)

		block := encodeBlock(t, data)
		err = agg.AddBlock(block)
		require.NoError(t, err)

		_, err = cli.ProcessStoreAgg(agg)
		require.NoError(t, err, "failed to store blocks")
	}

	// get keys of generated tlog data
	keys := make(map[string]struct{})

	log.Info("get keys of generated tlog data")
	for wr := range cli.Walk(0, tlog.TimeNowTimestamp()) {
		keys[string(wr.StorKey)] = struct{}{}
	}

	// delete data
	log.Info("delete tlog data")
	err = cli.Delete()
	require.NoError(t, err, "tlog delete failed")

	// make sure the data is not exist anymore
	log.Info("make sure the generated data are not exists anymore")
	for key := range keys {
		_, _, err = cli.storClient.Read([]byte(key))
		require.Errorf(t, err, "key = %v", key)
	}
}
