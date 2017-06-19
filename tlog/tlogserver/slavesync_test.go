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
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
	"github.com/zero-os/0-Disk/tlog/tlogserver/slavesync"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

// TestSlaveSyncWrite test the tlogserver ability to sync the ardb slave.
// With failed operation during writing to master
// It simulates the failing master by closing the master's redis pool
// in the middle of writing the data
func TestSlaveSyncRealWrite(t *testing.T) {
	testSlaveSyncReal(t, false)
}

func TestSlaveSyncRealRead(t *testing.T) {
	testSlaveSyncReal(t, true)
}

func testSlaveSyncReal(t *testing.T, isRead bool) {
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

	slavePool, slavePoolConfPath, err := newRedisPoolAndConfig(vdiskID, blockSize, size, nil)
	assert.Nil(t, err)

	defer slavePool.Close()
	defer os.Remove(slavePoolConfPath)

	// slave syncer manager
	tlogConf.AggMq = aggmq.NewMQ()
	tlogConf.ConfigPath = slavePoolConfPath
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

	t.Logf("== start nbdserver backend with tlogrcp=%v and slave pool", tlogrpc)
	nbdStor, backend1, err := newTestNbdBackend(ctx, t, vdiskID, tlogrpc, blockSize, size, slavePool)
	assert.Nil(t, err)

	defer backend1.Close(ctx)

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
		if !isRead && i == blocks/2 {
			t.Log("CLOSING nbdstor")
			nbdStor.Close()
		}

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
		log.Errorf("flush failed:%v", err)
		return
	}

	if isRead {
		t.Log("CLOSING nbdstor")
		nbdStor.Close()
	}

	t.Log("== validate all data are correct == ")

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := backend1.ReadAt(ctx, int64(offset), int64(blockSize))
		if !assert.Nil(t, err) {
			log.Errorf("read failed:%v", err)
			return
		}
		if !assert.Equal(t, data[offset:offset+blockSize], content) {
			log.Error("read doesn't return valid data")
			return
		}
	}

}

// creates in memory redis pool and generate config that resemble this pool
// it also generate config for slave if slave pool is not nil
func newRedisPoolAndConfig(vdiskID string, blockSize, size uint64,
	slavePool *redisstub.MemoryRedis) (*redisstub.MemoryRedis, string, error) {
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
	if slavePool != nil {
		nbdConf.StorageClusters["slave"] = config.StorageClusterConfig{
			DataStorage: []config.StorageServerConfig{
				config.StorageServerConfig{Address: slavePool.Address()},
			},
			MetadataStorage: &config.StorageServerConfig{Address: slavePool.Address()},
		}
		vdiskConf := nbdConf.Vdisks[vdiskID]
		vdiskConf.SlaveStorageCluster = "slave"
		nbdConf.Vdisks[vdiskID] = vdiskConf
	}

	// serialize the config to file
	if _, err := nbdConfFile.Write([]byte(nbdConf.String())); err != nil {
		return nil, "", err
	}
	return stor, nbdConfFile.Name(), nil
}

// create a new backend, used for writing.
// it returns the master pool and the nbd backend
func newTestNbdBackend(ctx context.Context, t *testing.T, vdiskID, tlogrpc string,
	blockSize, size uint64, slavePool *redisstub.MemoryRedis) (*redisstub.MemoryRedis, nbd.Backend, error) {

	masterPool, configPath, err := newRedisPoolAndConfig(vdiskID, blockSize, size, slavePool)
	if err != nil {
		return nil, nil, err
	}

	backend, err := newTestNbdBackendWithConfigPath(ctx, t, vdiskID, tlogrpc, blockSize, size, configPath)
	return masterPool, backend, err
}

func newTestNbdBackendWithConfigPath(ctx context.Context, t *testing.T, vdiskID, tlogrpc string,
	blockSize, size uint64, configPath string) (nbd.Backend, error) {

	// redis pool
	redisPool := ardb.NewRedisPool(nil)

	hotreloader, err := config.NewHotReloader(configPath, config.NBDServer)
	if err != nil {
		return nil, err
	}
	go hotreloader.Listen(ctx)

	config := ardb.BackendFactoryConfig{
		Pool:              redisPool,
		ConfigHotReloader: hotreloader,
		LBACacheLimit:     1024 * 1024,
		TLogRPCAddress:    tlogrpc,
		ConfigPath:        configPath,
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
