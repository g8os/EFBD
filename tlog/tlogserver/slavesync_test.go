package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/gonbdserver/nbd"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/ardb"
	"github.com/zero-os/0-Disk/redisstub"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/tlogclient"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
	"github.com/zero-os/0-Disk/tlog/tlogserver/slavesync"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

// TestSlaveSyncBasic test slave syncer in a 'nice' environment.
func TestSlaveSyncBasic(t *testing.T) {
	ctx := context.Background()

	const (
		vdiskID   = "myimg"
		blockSize = 4096
		size      = 1024 * 64
		firstSeq  = 0
	)

	t.Log("== Start tlogserver with slave syncer== ")
	tlogConf := server.DefaultConfig()
	tlogConf.ListenAddr = ""

	t.Log("create slave syncer")
	t.Log("create inmemory redis pool for ardb slave")
	ssPool, ssPoolConfPath, err := newArdbPool(vdiskID, blockSize, size)
	assert.Nil(t, err)
	defer ssPool.Close()
	defer os.Remove(ssPoolConfPath)

	// slave syncer manager
	tlogConf.AggMq = aggmq.NewMQ()
	tlogConf.ConfigPath = ssPoolConfPath
	ssm := slavesync.NewManager(tlogConf.AggMq, tlogConf.ConfigPath)
	go ssm.Run()

	t.Log("create inmemory redis pool for tlog")
	tlogPoolFact := tlog.InMemoryRedisPoolFactory(tlogConf.RequiredDataServers())

	tlogS, err := server.NewServer(tlogConf, tlogPoolFact)
	assert.Nil(t, err)

	t.Log("start the server")
	go tlogS.Listen()

	tlogrpc := tlogS.ListenAddr()
	t.Logf("listen addr = %v", tlogrpc)

	t.Logf("== start nbdserver backend with tlogrcp=%v", tlogrpc)
	backend1, err := newTestNbdBackend(ctx, t, vdiskID, tlogrpc, blockSize, size)
	assert.Nil(t, err)

	t.Log("== generate some random data and write it to the nbdserver backend== ")

	data := make([]byte, size)
	_, err = crand.Read(data)
	if !assert.Nil(t, err) {
		return
	}
	blocks := size / blockSize

	zeroBlock := make([]byte, blockSize)

	for i := 0; i < blocks; i++ {
		offset := i * blockSize

		op := mrand.Int() % 10

		if op > 5 && op < 8 { // zero block
			_, err := backend1.WriteZeroesAt(ctx, int64(offset), int64(blockSize))
			if !assert.Nil(t, err) {
				return
			}

			copy(data[offset:], zeroBlock)
			continue
		}

		if op > 8 {
			// partial zero block
			r := mrand.Int()
			size := r % (blockSize / 2)
			offset := offset + (r % (blockSize / 4))
			copy(data[offset:], zeroBlock[:size])
		}

		_, err := backend1.WriteAt(ctx, data[offset:offset+blockSize], int64(offset))
		if !assert.Nil(t, err) {
			return
		}
	}

	t.Log("flush data")
	err = backend1.Flush(ctx)
	if !assert.Nil(t, err) {
		return
	}

	t.Log("== validate all data are correct == ")

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := backend1.ReadAt(ctx, int64(offset), int64(blockSize))
		if !assert.Nil(t, err) {
			return
		}
		if !assert.Equal(t, data[offset:offset+blockSize], content) {
			return
		}
	}

	t.Log("== send command to tlog to sync the slave==")
	// we can't do it from the backend,
	// but fortunately it is OK to use another tlog client
	// our tlog server should support multiple clients (in a limited way)
	client, err := tlogclient.New(tlogrpc, vdiskID, 0, false)
	assert.Nil(t, err)

	err = client.WaitNbdSlaveSync()
	assert.Nil(t, err)

	t.Log("Start another nbdserver with slave pool")
	t.Log("== validate all data are correct == ")
}

func newArdbPool(vdiskID string, blockSize, size uint64) (*redisstub.MemoryRedis, string, error) {
	stor := redisstub.NewMemoryRedis()

	// create conf file
	nbdConfFile, err := ioutil.TempFile("", "zerodisk")
	if err != nil {
		return nil, "", err
	}

	go stor.Listen()

	nbdConf := config.Config{
		Vdisks: map[string]config.VdiskConfig{
			vdiskID: config.VdiskConfig{
				BlockSize:          blockSize,
				ReadOnly:           false,
				Size:               size,
				StorageCluster:     "mycluster",
				Type:               config.VdiskTypeBoot,
				TlogSlaveSync:      true,
				TlogStorageCluster: "tlogCluster",
			},
		},
		StorageClusters: map[string]config.StorageClusterConfig{
			"mycluster": config.StorageClusterConfig{
				DataStorage: []config.StorageServerConfig{
					config.StorageServerConfig{Address: stor.Address()},
				},
				MetadataStorage: &config.StorageServerConfig{Address: stor.Address()},
			},
			"tlogCluster": config.StorageClusterConfig{ // dummy cluster, not really used
				DataStorage: []config.StorageServerConfig{
					config.StorageServerConfig{Address: stor.Address()},
					config.StorageServerConfig{Address: stor.Address()},
					config.StorageServerConfig{Address: stor.Address()},
					config.StorageServerConfig{Address: stor.Address()},
					config.StorageServerConfig{Address: stor.Address()},
					config.StorageServerConfig{Address: stor.Address()},
				},
			},
		},
	}

	// serialize the config to file
	if _, err := nbdConfFile.Write([]byte(nbdConf.String())); err != nil {
		return nil, "", err
	}
	return stor, nbdConfFile.Name(), nil
}

// create a new backend, used for writing
func newTestNbdBackend(ctx context.Context, t *testing.T, vdiskID, tlogrpc string, blockSize, size uint64) (nbd.Backend, error) {
	_, configPath, err := newArdbPool(vdiskID, blockSize, size)
	if err != nil {
		return nil, err
	}

	// redis pool
	redisPool := ardb.NewRedisPool(nil)

	hotreloader, err := config.NopHotReloader(configPath, config.NBDServer)
	if err != nil {
		return nil, err
	}

	config := ardb.BackendFactoryConfig{
		Pool:              redisPool,
		ConfigHotReloader: hotreloader,
		LBACacheLimit:     ardb.DefaultLBACacheLimit,
		TLogRPCAddress:    tlogrpc,
	}
	fact, err := ardb.NewBackendFactory(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create factory:%v", err)
	}

	ec := &nbd.ExportConfig{
		Name:        vdiskID,
		Description: "zero-os/zerodisk",
		Driver:      "ardb",
		ReadOnly:    false,
		TLSOnly:     false,
	}

	backend, err := fact.NewBackend(ctx, ec)
	if err != nil {
		return nil, err
	}
	go backend.GoBackground(ctx)
	return backend, nil
}
