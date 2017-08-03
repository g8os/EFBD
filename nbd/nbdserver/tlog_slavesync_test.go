package main

/*
		// ARDB SLAVE SWAP IS DISABLED SINCE MILESTONE 6
		// SEE: https://github.com/zero-os/0-Disk/issues/357

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
	if !assert.NoError(t, err) {
		return
	}

	defer slavePool.Close()
	defer os.Remove(slavePoolConfPath)

	// slave syncer manager
	tlogConf.AggMq = aggmq.NewMQ()
	tlogConf.ConfigInfo = zerodisk.ConfigInfo{
		Resource:     slavePoolConfPath,
		ResourceType: zerodisk.FileConfigResource,
	}
	ssm := slavesync.NewManager(ctx, tlogConf.AggMq, tlogConf.ConfigInfo)
	go ssm.Run()

	t.Log("create inmemory redis pool for tlog")
	tlogPoolFact := tlog.InMemoryRedisPoolFactory(tlogConf.RequiredDataServers())

	tlogS, err := server.NewServer(tlogConf, tlogPoolFact)
	if !assert.NoError(t, err) {
		return
	}

	t.Log("start the server")
	go tlogS.Listen(ctx)

	tlogrpc := tlogS.ListenAddr()
	t.Logf("listen addr = %v", tlogrpc)

	t.Logf("== start tlog storage with tlogrcp=%v and slave pool", tlogrpc)
	storage1, masterProvider, err := newTlogWithNonDedupedStorage(ctx, vdiskID, slavePoolConfPath, size, blockSize, tlogrpc)
	if !assert.NoError(t, err) {
		return
	}
	defer masterProvider.Close()
	defer storage1.Close()

	t.Log("== generate some random data and write it to the tlog storage== ")

	data := make([]byte, size)
	_, err = crand.Read(data)
	if !assert.NoError(t, err) {
		return
	}
	blocks := size / blockSize

	zeroBlock := make([]byte, blockSize)

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		if !isRead && i == blocks/2 {
			t.Log("CLOSING nbdstor")
			masterProvider.Close()
		}

		op := mrand.Int() % 10

		if op > 5 && op < 8 { // zero block
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

		err = storage1.SetBlock(int64(i), data[offset:offset+blockSize])
		if !assert.NoError(t, err) {
			return
		}
	}

	t.Log("flush data")
	err = storage1.Flush()
	if !assert.NoError(t, err) {
		log.Errorf("flush failed:%v", err)
		return
	}

	if isRead {
		t.Log("CLOSING nbdstor")
		masterProvider.Close()
	}

	t.Log("== validate all data are correct == ")

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := storage1.GetBlock(int64(i))
		if !assert.Nil(t, err) {
			log.Errorf("read failed:%v", err)
			return
		}
		if !assert.Equal(t, normalizeTestBlock(data[offset:offset+blockSize]), content) {
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
// - both tlog use same tlogpool
// - it proves that even slave syncer started late, the slave still synced
func TestSlaveSyncRestart(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	ctxTlog1, cancelFuncTlog1 := context.WithCancel(context.Background())
	defer cancelFuncTlog1()

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
	if !assert.NoError(t, err) {
		return
	}

	defer slavePool.Close()
	defer metaPool.Close()
	defer os.Remove(tlogConfPath)

	tlogConf := server.DefaultConfig()
	tlogConf.ListenAddr = ""
	tlogConf.K = 1
	tlogConf.M = 1
	tlogConf.ConfigInfo = zerodisk.ConfigInfo{
		Resource:     tlogConfPath,
		ResourceType: zerodisk.FileConfigResource,
	}

	t.Log("create inmemory redis pool for tlog")

	// create any kind of valid pool factory
	tlogPoolFact, err := tlog.AnyRedisPoolFactory(ctx, tlog.RedisPoolFactoryConfig{
		RequiredDataServerCount: tlogConf.RequiredDataServers(),
		ConfigInfo:              tlogConf.ConfigInfo,
		AutoFill:                true,
		AllowInMemory:           true,
	})
	if err != nil {
		log.Fatalf("failed to create redis pool factory: %s", err.Error())
	}

	tlogS, err := server.NewServer(tlogConf, tlogPoolFact)
	if !assert.NoError(t, err) {
		return
	}

	t.Log("start the server")
	go tlogS.Listen(ctxTlog1)

	tlogrpc := tlogS.ListenAddr()
	t.Logf("listen addr = %v", tlogrpc)

	t.Logf("== start tlog storage with tlogrcp=%v and slave pool", tlogrpc)
	master, nbdConfPath, err := newRedisPoolAndConfig(vdiskID, blockSize, size, slavePool)
	assert.NoError(t, err)

	storage1, masterProvider, err := newTlogWithNonDedupedStorage(
		ctx, vdiskID, tlogConf.ConfigInfo, size, blockSize, tlogrpc)
	if !assert.NoError(t, err) {
		return
	}
	defer masterProvider.Close()

	t.Log("== generate some random data and write it to the tlog storage == ")

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

		err := storage1.SetBlock(int64(i), data[offset:offset+blockSize])
		if !assert.NoError(t, err) {
			return
		}
	}

	t.Log("flush data")
	err = storage1.Flush()
	if !assert.NoError(t, err) {
		log.Errorf("flush failed:%v", err)
		return
	}

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := storage1.GetBlock(int64(i))
		if !assert.Nil(t, err) {
			return
		}
		if !assert.Equal(t, normalizeTestBlock(data[offset:offset+blockSize]), content, "block: %v", i) {
			return
		}
	}

	storage1.Close()
	masterProvider.Close()
	cancelFuncTlog1()

	//----------------------------------------------------------
	// start nbd2(master, slave) with tlog(tlogPool) with slave
	// - close master
	// -

	// slave syncer manager
	tlogConf.AggMq = aggmq.NewMQ()
	ssm := slavesync.NewManager(ctx, tlogConf.AggMq, tlogConf.ConfigInfo)
	go ssm.Run()

	tlogS2, err := server.NewServer(tlogConf, tlogPoolFact)
	if !assert.NoError(t, err) {
		return
	}
	go tlogS2.Listen(ctx)

	storage2, masterProvider, err := newTlogWithNonDedupedStorage(
		ctx, vdiskID, tlogConf.ConfigInfo, size, blockSize, tlogS2.ListenAddr())
	if !assert.NoError(t, err) {
		return
	}
	defer storage2.Close()
	defer masterProvider.Close()

	t.Log("Close the master")
	master.Close()

	// nbd2 use slave, try the read opertion
	t.Log("== validate all data is correct == ")

	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := storage2.GetBlock(int64(i))
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Equal(t, normalizeTestBlock(data[offset:offset+blockSize]), content, "block: %v", i) {
			return
		}
	}
}

func newTlogWithNonDedupedStorage(ctx context.Context, vdiskID string, configInfo zerodisk.ConfigInfo, vdiskSize, blockSize int64, tlogrpc string) (storage.BlockStorage, ardb.ConnProvider, error) {
	// create redis provider
	redisProvider, err := ardb.DynamicProvider(ctx, vdiskID, configInfo, nil)
	if err != nil {
		return nil, nil, err
	}

	storage, err := storage.NonDeduped(
		vdiskID, "", blockSize, false, redisProvider)
	if err != nil {
		redisProvider.Close()
		return nil, nil, err
	}

	storage, err = newTlogStorage(vdiskID, tlogrpc, nil, blockSize, storage)
	if err != nil {
		redisProvider.Close()
		return nil, nil, err
	}

	return storage, redisProvider, nil
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
				StorageCluster: "tlogCluster",
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
				StorageCluster: "tlogCluster",
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

*/
