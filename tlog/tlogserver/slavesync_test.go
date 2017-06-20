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
	ctx, cancelFunc := context.WithCancel(context.Background())

	defer cancelFunc()

	const (
		vdiskID   = "myimg"
		blockSize = 4096
		size      = 1024 * 64
		firstSeq  = 0
	)

	t.Log("== Start tlogserver with slave syncer== ")
	tlogConf := server.DefaultConfig()
	tlogConf.ListenAddr = ""
	tlogConf.K = 1
	tlogConf.M = 1

	t.Log("create slave syncer")
	t.Log("create inmemory redis pool for ardb slave")

	slavePool, slavePoolConfPath, err := newRedisPoolAndConfig(vdiskID, blockSize, size, nil)
	assert.Nil(t, err)

	defer slavePool.Close()
	defer os.Remove(slavePoolConfPath)

	// slave syncer manager
	tlogConf.AggMq = aggmq.NewMQ()
	tlogConf.ConfigPath = slavePoolConfPath
	ssm := slavesync.NewManager(ctx, tlogConf.AggMq, tlogConf.ConfigPath)
	go ssm.Run()

	t.Log("create inmemory redis pool for tlog")
	tlogPoolFact := tlog.InMemoryRedisPoolFactory(tlogConf.RequiredDataServers())

	tlogS, err := server.NewServer(tlogConf, tlogPoolFact)
	assert.Nil(t, err)

	t.Log("start the server")
	go tlogS.Listen(ctx)

	tlogrpc := tlogS.ListenAddr()
	t.Logf("listen addr = %v", tlogrpc)

	t.Logf("== start nbdserver backend with tlogrcp=%v and slave pool", tlogrpc)
	_, masterPool, backend1, err := newTestNbdBackend(ctx, t, vdiskID, tlogrpc, blockSize, size, slavePool)
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
			masterPool.Close()
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
		masterPool.Close()
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

//	start nbd1(master,slave) with tlog(tlogPool) without slave sync
// - do write operation
// - close nbd1
// start nbd2(master, slave) with tlog2(tlogPool) with slave sync
// - close master
// nbd2 use slave, try the read opertion
// notes:
// - we use two tlog the first one without, the later with slave sync
// - both tlog same tlogpool
// - it proves that even slave syncer started late, the slave still synced
// TODO : we disabled it because of high memory usage (6GB)
//        the solution is to create more memory efficient redis pool
func TestSlaveSyncRestart(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	ctxTlog1, cancelFuncTlog1 := context.WithCancel(context.Background())

	const (
		vdiskID   = "myimg"
		blockSize = 4096
		size      = 1024 * 64
		firstSeq  = 0
	)

	// ----------------------------
	//	start nbd1(master,slave) with tlog(tlogPool) without slave sync
	// - do write operation
	// - close nbd1
	// ---------------------------

	t.Log("== Start tlogserver with slave syncer== ")

	t.Log("create inmemory redis pool for ardb slave")
	slavePool, metaPool, tlogConfPath, err := newTlogRedisPoolAndConfig(vdiskID, blockSize, size)
	assert.Nil(t, err)
	defer slavePool.Close()
	defer metaPool.Close()
	defer os.Remove(tlogConfPath)

	tlogConf := server.DefaultConfig()
	tlogConf.ListenAddr = ""
	tlogConf.K = 1
	tlogConf.M = 1
	tlogConf.ConfigPath = tlogConfPath

	t.Log("create inmemory redis pool for tlog")
	serverConfigs, err := config.ParseCSStorageServerConfigStrings("")
	if err != nil {
		log.Fatal(err)
	}

	// create any kind of valid pool factory
	tlogPoolFact, err := tlog.AnyRedisPoolFactory(tlog.RedisPoolFactoryConfig{
		RequiredDataServerCount: tlogConf.RequiredDataServers(),
		ConfigPath:              tlogConf.ConfigPath,
		ServerConfigs:           serverConfigs,
		AutoFill:                true,
		AllowInMemory:           true,
	})
	if err != nil {
		log.Fatalf("failed to create redis pool factory: %s", err.Error())
	}

	tlogS, err := server.NewServer(tlogConf, tlogPoolFact)
	assert.Nil(t, err)

	t.Log("start the server")
	go tlogS.Listen(ctxTlog1)

	tlogrpc := tlogS.ListenAddr()
	t.Logf("listen addr = %v", tlogrpc)

	t.Logf("== start nbdserver backend with tlogrcp=%v and slave pool", tlogrpc)
	nbdConfPath, master, backend1, err := newTestNbdBackend(ctx, t, vdiskID, tlogrpc, blockSize, size, slavePool)
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
		log.Errorf("flush failed:%v", err)
		return
	}
	backend1.Close(ctx)
	cancelFuncTlog1()

	//----------------------------------------------------------
	// start nbd2(master, slave) with tlog(tlogPool) with slave
	// - close master
	// -

	// slave syncer manager
	tlogConf.AggMq = aggmq.NewMQ()
	ssm := slavesync.NewManager(ctx, tlogConf.AggMq, tlogConf.ConfigPath)
	go ssm.Run()

	tlogS2, err := server.NewServer(tlogConf, tlogPoolFact)
	assert.Nil(t, err)
	go tlogS2.Listen(ctx)

	backend2, err := newTestNbdBackendWithConfigPath(ctx, t, vdiskID, tlogS2.ListenAddr(), blockSize,
		size, nbdConfPath)
	defer backend2.Close(ctx)

	t.Log("Close the master")
	master.Close()

	// nbd2 use slave, try the read opertion
	t.Log("== validate all data are correct == ")

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := backend2.ReadAt(ctx, int64(offset), int64(blockSize))
		if !assert.Nil(t, err) {
			return
		}
		if !assert.Equal(t, data[offset:offset+blockSize], content) {
			return
		}
	}

}

func newTlogRedisPoolAndConfig(vdiskID string, blockSize, size uint64) (*redisstub.MemoryRedis, *redisstub.MemoryRedis, string, error) {
	stor1 := redisstub.NewMemoryRedis()
	stor2 := redisstub.NewMemoryRedis()

	// create conf file
	confFile, err := ioutil.TempFile("", "zerodisk")
	if err != nil {
		return nil, nil, "", err
	}

	go stor1.Listen()
	go stor2.Listen()

	conf := config.Config{
		Vdisks: map[string]config.VdiskConfig{
			vdiskID: config.VdiskConfig{
				BlockSize:          blockSize,
				ReadOnly:           false,
				Size:               size,
				StorageCluster:     "mycluster",
				Type:               config.VdiskTypeDB,
				TlogSlaveSync:      true,
				TlogStorageCluster: "tlogCluster",
			},
		},
		StorageClusters: map[string]config.StorageClusterConfig{
			"mycluster": config.StorageClusterConfig{
				DataStorage: []config.StorageServerConfig{
					config.StorageServerConfig{Address: stor1.Address()},
				},
				MetadataStorage: &config.StorageServerConfig{Address: stor1.Address()},
			},
			"tlogCluster": config.StorageClusterConfig{ // dummy cluster, not really used
				DataStorage: []config.StorageServerConfig{
					config.StorageServerConfig{Address: stor1.Address()},
					config.StorageServerConfig{Address: stor2.Address()},
				},
			},
		},
	}

	// serialize the config to file
	if _, err := confFile.Write([]byte(conf.String())); err != nil {
		return nil, nil, "", err
	}
	return stor1, stor2, confFile.Name(), nil
}

// creates in memory redis pool nbd server and generate config that resemble this pool
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
				Type:               config.VdiskTypeDB,
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
	blockSize, size uint64, slavePool *redisstub.MemoryRedis) (string, *redisstub.MemoryRedis, nbd.Backend, error) {

	masterPool, configPath, err := newRedisPoolAndConfig(vdiskID, blockSize, size, slavePool)
	if err != nil {
		return "", nil, nil, err
	}

	backend, err := newTestNbdBackendWithConfigPath(ctx, t, vdiskID, tlogrpc, blockSize, size, configPath)
	return configPath, masterPool, backend, err
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
