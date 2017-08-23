package tlogclient

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/meta/embedserver"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

var (
	testConf = server.DefaultConfig()
)

func init() {
	testConf.ListenAddr = "127.0.0.1:0"
	testConf.K = 1
	testConf.M = 1
}

func newZeroStorDefaultConfig(t *testing.T, vdiskID string) (func(), *config.StubSource, stor.Config) {
	return newZeroStorConfig(t, vdiskID, testConf.PrivKey, testConf.K, testConf.M)
}

func newZeroStorConfig(t *testing.T, vdiskID, privKey string,
	data, parity int) (func(), *config.StubSource, stor.Config) {

	// stor server
	storCluster, err := embeddedserver.NewZeroStorCluster(data + parity)
	require.Nil(t, err)

	// meta server
	mdServer, err := embedserver.New()
	require.Nil(t, err)

	return newZeroStorConfigFromCluster(t, storCluster, mdServer, vdiskID, privKey, data, parity)
}

func newZeroStorConfigFromCluster(t *testing.T, storCluster *embeddedserver.ZeroStorCluster,
	mdServer *embedserver.Server, vdiskID, privKey string, data,
	parity int) (func(), *config.StubSource, stor.Config) {

	var servers []config.ServerConfig
	for _, addr := range storCluster.Addrs() {
		servers = append(servers, config.ServerConfig{
			Address: addr,
		})
	}

	storConf := stor.Config{
		VdiskID:         vdiskID,
		Organization:    os.Getenv("iyo_organization"),
		Namespace:       "thedisk",
		IyoClientID:     os.Getenv("iyo_client_id"),
		IyoSecret:       os.Getenv("iyo_secret"),
		ZeroStorShards:  storCluster.Addrs(),
		MetaShards:      []string{mdServer.ListenAddr()},
		DataShardsNum:   data,
		ParityShardsNum: parity,
		EncryptPrivKey:  privKey,
	}

	clusterID := "zero_stor_cluster_id"
	stubSource := config.NewStubSource()

	stubSource.SetTlogZeroStorCluster(vdiskID, clusterID, &config.ZeroStorClusterConfig{
		IYO: config.IYOCredentials{
			Org:       storConf.Organization,
			Namespace: storConf.Namespace,
			ClientID:  storConf.IyoClientID,
			Secret:    storConf.IyoSecret,
		},
		Servers: servers,
		MetadataServers: []config.ServerConfig{
			config.ServerConfig{
				Address: mdServer.ListenAddr(),
			},
		},
	})

	cleanFunc := func() {
		mdServer.Stop()
		storCluster.Close()
	}
	return cleanFunc, stubSource, storConf
}

func waitForBlockReceivedResponse(t *testing.T, client *Client, minSequence, maxSequence uint64) {
	// map of sequence we want to wait for the response to come
	logsToRecv := map[uint64]struct{}{}
	for i := minSequence; i <= maxSequence; i++ {
		logsToRecv[i] = struct{}{}
	}

	respChan := client.Recv()

	for len(logsToRecv) > 0 {
		// recv
		resp := <-respChan

		if resp.Err == nil {
			// check response content
			response := resp.Resp
			if response == nil {
				continue
			}
			switch response.Status {
			case tlog.BlockStatusRecvOK:
				require.Equal(t, 1, len(response.Sequences))
				delete(logsToRecv, response.Sequences[0])
			case tlog.BlockStatusFlushOK: // if flushed, it means all previous already received
				maxSeq := response.Sequences[len(response.Sequences)-1]
				var seq uint64
				for seq = 0; seq <= maxSeq; seq++ {
					delete(logsToRecv, seq)
				}
			}
		}
	}
}

func testClientSend(t *testing.T, client *Client, startSeq, endSeq uint64, data []byte) {
	for x := startSeq; x <= endSeq; x++ {
		err := client.Send(schema.OpSet, x, int64(x), x, data)
		require.Nil(t, err)
	}

}
func testClientWaitSeqFlushed(ctx context.Context, t *testing.T, respChan <-chan *Result,
	cancelFunc func(), seqWait uint64) (finished bool) {

	for {
		select {
		case <-time.After(20 * time.Second):
			t.Fatal("testClientWaitSeqFlushed timeout")
		case re := <-respChan:
			if re.Err != nil {
				t.Logf("recv err = %v", re.Err)
				return
			}
			status := re.Resp.Status
			if status < 0 {
				continue
			}

			if status == tlog.BlockStatusFlushOK {
				seqs := re.Resp.Sequences
				seq := seqs[len(seqs)-1]

				if seq >= seqWait { // we've received all sequences
					finished = true
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
