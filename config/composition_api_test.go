package config

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
)

func TestReadNBDStorageConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadNBDStorageConfig(nil, "")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(expected Key) {
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = ReadNBDStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadNBDStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey(Key{ID: "a", Type: KeyVdiskNBD})

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default
	source.SetPrimaryStorageCluster("a", "mycluster", nil)
	_, err = ReadNBDStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to nil reference")
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterStorage})

	source.SetStorageCluster("mycluster", new(StorageClusterConfig))
	_, err = ReadNBDStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to invalid reference")
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterStorage})

	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
	})
	nbdStorageCfg, err := ReadNBDStorageConfig(source, "a")
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(
			[]StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
			nbdStorageCfg.StorageCluster.DataStorage)
	}

	source.SetTemplateStorageCluster("a", "templateCluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16380"},
			StorageServerConfig{Address: "localhost:16381"},
			StorageServerConfig{Address: "localhost:16382"},
		},
	})
	nbdStorageCfg, err = ReadNBDStorageConfig(source, "a")
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(
			[]StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
			nbdStorageCfg.StorageCluster.DataStorage)
		if assert.NotNil(nbdStorageCfg.TemplateStorageCluster) {
			assert.Equal(
				[]StorageServerConfig{
					StorageServerConfig{Address: "localhost:16380"},
					StorageServerConfig{Address: "localhost:16381"},
					StorageServerConfig{Address: "localhost:16382"},
				},
				nbdStorageCfg.TemplateStorageCluster.DataStorage)
		}
	}

	source.SetSlaveStorageCluster("a", "slaveCluster", nil)
	_, err = ReadNBDStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to nil slave cluster")
	testInvalidKey(Key{ID: "slaveCluster", Type: KeyClusterStorage})

	source.SetSlaveStorageCluster("a", "slaveCluster", new(StorageClusterConfig))
	_, err = ReadNBDStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to invalid slave cluster")
	testInvalidKey(Key{ID: "slaveCluster", Type: KeyClusterStorage})

	source.SetSlaveStorageCluster("a", "slaveCluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
			StorageServerConfig{Address: "localhost:16380"},
		},
	})

	nbdStorageCfg, err = ReadNBDStorageConfig(source, "a")
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(
			[]StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
			nbdStorageCfg.StorageCluster.DataStorage)
		if assert.NotNil(nbdStorageCfg.TemplateStorageCluster) {
			assert.Equal(
				[]StorageServerConfig{
					StorageServerConfig{Address: "localhost:16380"},
					StorageServerConfig{Address: "localhost:16381"},
					StorageServerConfig{Address: "localhost:16382"},
				},
				nbdStorageCfg.TemplateStorageCluster.DataStorage)
		}
		if assert.NotNil(nbdStorageCfg.SlaveStorageCluster) {
			assert.Equal(
				[]StorageServerConfig{
					StorageServerConfig{Address: "localhost:16379"},
					StorageServerConfig{Address: "localhost:16380"},
				},
				nbdStorageCfg.SlaveStorageCluster.DataStorage)
		}
	}
}

func TestReadTlogStorageConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadTlogStorageConfig(nil, "")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(expected Key) {
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = ReadTlogStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadTlogStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey(Key{ID: "a", Type: KeyVdiskTlog})

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default

	source.SetTlogZeroStorCluster("a", "mycluster", new(ZeroStorClusterConfig))
	_, err = ReadTlogStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to invalid reference")
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterZeroStor})

	// set zerostor cluster
	originalZeroStorCfg := ZeroStorClusterConfig{
		IYO: IYOCredentials{
			Org:       "foo org",
			Namespace: "foo namespace",
			ClientID:  "foo clientID",
			Secret:    "foo secret",
		},
		Servers:         []ServerConfig{ServerConfig{"1.1.1.1:11"}},
		MetadataServers: []ServerConfig{ServerConfig{"2.2.2.2:22"}},
	}
	source.SetTlogZeroStorCluster("a", "mycluster", &originalZeroStorCfg)
	tlogStorCfg, err := ReadTlogStorageConfig(source, "a")
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(originalZeroStorCfg, tlogStorCfg.ZeroStorCluster)
	}

	// add nil slave cluster
	source.SetSlaveStorageCluster("a", "slaveCluster", nil)

	_, err = ReadTlogStorageConfig(source, "a")
	assert.Error(err, "should trigger error due to nil slave cluster")
	testInvalidKey(Key{ID: "slaveCluster", Type: KeyClusterStorage})

	// add slave cluster
	slaveClusterCfg := StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:3000"},
			StorageServerConfig{Address: "localhost:3002", Database: 42},
		},
	}
	source.SetSlaveStorageCluster("a", "slaveCluster", &slaveClusterCfg)

	tlogStorCfg, err = ReadTlogStorageConfig(source, "a")
	if assert.NoError(err, "should be fine as both the vdisk and storages are properly configured") {
		assert.Equal(originalZeroStorCfg, tlogStorCfg.ZeroStorCluster)
		assert.Equal(slaveClusterCfg, *tlogStorCfg.SlaveStorageCluster)
	}
}

func TestWatchNBDStorageConfig_FailAtStartup(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := WatchNBDStorageConfig(ctx, nil, "a")
	assert.Equal(err, ErrNilSource)

	source := NewStubSource()

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(expected Key) {
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = WatchNBDStorageConfig(ctx, source, "")
	assert.Equal(err, ErrNilID)

	_, err = WatchNBDStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing configs")

	source.SetPrimaryStorageCluster("a", "mycluster", nil)
	_, err = WatchNBDStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing/nil configs")
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterStorage})

	// last one is golden
	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	})
	_, err = WatchNBDStorageConfig(ctx, source, "a")
	assert.NoError(err, "should be fine now")
}

func TestWatchNBDStorageConfig_FailAfterSuccess(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := NewStubSource()

	source.SetPrimaryStorageCluster("a", "mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	})
	ch, err := WatchNBDStorageConfig(ctx, source, "a")
	if !assert.NoError(err, "should be valid") {
		return
	}

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(expected Key) {
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		}
	}

	output := <-ch
	assert.Nil(output.TemplateStorageCluster)
	if assert.Len(output.StorageCluster.DataStorage, 1) {
		assert.Equal(
			StorageServerConfig{Address: "localhost:16379"},
			output.StorageCluster.DataStorage[0])
	}

	// now let's break it
	source.SetStorageCluster("mycluster", nil)
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterStorage})

	// now let's fix it again
	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16380"},
		},
	})

	// trigger reload (even though it was broken before)
	output = <-ch
	assert.Nil(output.TemplateStorageCluster)
	if assert.Len(output.StorageCluster.DataStorage, 1) {
		assert.Equal(
			StorageServerConfig{Address: "localhost:16380"},
			output.StorageCluster.DataStorage[0])
	}
}

func TestWatchNBDStorageConfig_ChangeClusterReference(t *testing.T) {
	assert := assert.New(t)

	source := NewStubSource()

	primaryStorageCluster := &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	}

	var templateStoragecluster *StorageClusterConfig
	var slaveStoragecluster *StorageClusterConfig

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source.SetPrimaryStorageCluster("a", "mycluster", primaryStorageCluster)
	ch, err := WatchNBDStorageConfig(ctx, source, "a")
	if !assert.NoError(err, "should be valid") {
		return
	}

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(expected Key) {
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	testValue := func() {
		select {
		case output := <-ch:
			assert.True(output.StorageCluster.Equal(primaryStorageCluster),
				"unexpected primary cluster: %v", output.StorageCluster)
			assert.True(output.TemplateStorageCluster.Equal(templateStoragecluster),
				"unexpected template cluster: %v", output.TemplateStorageCluster)
			assert.True(output.SlaveStorageCluster.Equal(slaveStoragecluster),
				"unexpected slave cluster: %v", output.SlaveStorageCluster)
		case invalidKey := <-invalidKeyCh:
			assert.FailNow("received unexpected invalid key", "%v", invalidKey)
		}
	}

	testValue()

	source.SetPrimaryStorageCluster("a", "foocluster", nil)
	testInvalidKey(Key{ID: "foocluster", Type: KeyClusterStorage}) // error value should not be updated

	primaryStorageCluster = &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16381"},
		},
	}

	// now let's actually set the storage cluster
	source.SetPrimaryStorageCluster("a", "foocluster", primaryStorageCluster)
	testValue()

	// now let's set template cluster
	templateStoragecluster = &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "231.201.201.200:2000"},
			StorageServerConfig{Address: "231.201.201.200:2020"},
		},
	}
	source.SetTemplateStorageCluster("a", "master", templateStoragecluster)
	testValue()

	// now let's set slave cluster
	slaveStoragecluster = &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "slave:200"},
			StorageServerConfig{Address: "slave:201"},
		},
	}
	source.SetSlaveStorageCluster("a", "slave", slaveStoragecluster)
	testValue()

	// do another stupid illegal action
	source.SetPrimaryStorageCluster("a", "primary", nil)
	// trigger reload
	testInvalidKey(Key{ID: "primary", Type: KeyClusterStorage}) // error value should not be updated

	primaryStorageCluster.DataStorage[0].Database = 3
	source.SetStorageCluster("primary", primaryStorageCluster)
	testValue() // updating a storage cluster should be ok

	templateStoragecluster = nil
	source.SetTemplateStorageCluster("a", "", templateStoragecluster)
	testValue() // and template storage cluster can even be dereferenced

	// cancel context
	cancel()
	// channel should be now closed
	select {
	case <-time.After(time.Millisecond * 500):
		assert.FailNow("timed out, ch doesn't seem to close")
	case _, open := <-ch:
		if !assert.False(open) {
			assert.FailNow("channel should have been closed")
		}
	}
}

func TestWatchTlogStorageConfig_FailAtStartup(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := WatchTlogStorageConfig(ctx, nil, "a")
	assert.Equal(err, ErrNilSource)

	source := NewStubSource()

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(expected Key) {
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = WatchTlogStorageConfig(ctx, source, "")
	assert.Equal(err, ErrNilID)

	_, err = WatchTlogStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing configs")

	source.SetTlogZeroStorCluster("a", "mycluster", nil)
	_, err = WatchTlogStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing/nil configs")
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterZeroStor})

	// last one is golden
	source.SetTlogZeroStorCluster("a", "mycluster", &ZeroStorClusterConfig{
		IYO: IYOCredentials{
			Org:       "foo org",
			Namespace: "foo namespace",
			ClientID:  "foo clientID",
			Secret:    "foo secret",
		},
		Servers:         []ServerConfig{ServerConfig{"1.1.1.1:11"}},
		MetadataServers: []ServerConfig{ServerConfig{"2.2.2.2:22"}},
	})

	_, err = WatchTlogStorageConfig(ctx, source, "a")
	assert.NoError(err, "should be fine now")
}

func TestWatchTlogStorageConfig_FailAfterSuccess(t *testing.T) {
	assert := assert.New(t)

	source := NewStubSource()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source.SetTlogZeroStorCluster("a", "mycluster", &ZeroStorClusterConfig{
		IYO: IYOCredentials{
			Org:       "foo org",
			Namespace: "foo namespace",
			ClientID:  "foo clientID",
			Secret:    "foo secret",
		},
		Servers:         []ServerConfig{ServerConfig{"1.1.1.1:11"}},
		MetadataServers: []ServerConfig{ServerConfig{"2.2.2.2:22"}},
	})
	ch, err := WatchTlogStorageConfig(ctx, source, "a")
	assert.NoError(err, "should be valid")

	output := <-ch
	assert.Nil(output.SlaveStorageCluster)
	if assert.Len(output.ZeroStorCluster.Servers, 1) {
		assert.Equal(
			ServerConfig{"1.1.1.1:11"},
			output.ZeroStorCluster.Servers[0])
	}

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(expected Key) {
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	// now let's break it
	source.SetZeroStorCluster("mycluster", nil)
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterZeroStor})

	// now let's fix it again
	source.SetTlogZeroStorCluster("a", "mycluster", &ZeroStorClusterConfig{
		IYO: IYOCredentials{
			Org:       "foo org",
			Namespace: "foo namespace",
			ClientID:  "foo clientID",
			Secret:    "foo secret",
		},
		Servers:         []ServerConfig{ServerConfig{"3.3.3.3:33"}},
		MetadataServers: []ServerConfig{ServerConfig{"2.2.2.2:22"}},
	})

	// trigger reload (even though it was broken before)
	output = <-ch
	assert.Nil(output.SlaveStorageCluster)
	if assert.Len(output.ZeroStorCluster.Servers, 1) {
		assert.Equal(
			ServerConfig{"3.3.3.3:33"},
			output.ZeroStorCluster.Servers[0])
	}
}

func TestWatchTlogStorageConfig_ChangeClusterReference(t *testing.T) {
	assert := assert.New(t)

	source := NewStubSource()

	zeroStorCluster := ZeroStorClusterConfig{
		IYO: IYOCredentials{
			Org:       "foo org",
			Namespace: "foo namespace",
			ClientID:  "foo clientID",
			Secret:    "foo secret",
		},
		Servers:         []ServerConfig{ServerConfig{"1.1.1.1:11"}},
		MetadataServers: []ServerConfig{ServerConfig{"2.2.2.2:22"}},
	}

	var slaveStoragecluster *StorageClusterConfig

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//	source.SetTlogStorageCluster("a", "mycluster", tlogStorageCluster)
	source.SetTlogZeroStorCluster("a", "mycluster", &zeroStorCluster)

	ch, err := WatchTlogStorageConfig(ctx, source, "a")
	if !assert.NoError(err, "should be valid") {
		return
	}

	invalidKeyCh := source.InvalidKey()

	testInvalidKey := func(expected Key) {
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	testValue := func() {
		select {
		case output := <-ch:
			assert.True(output.SlaveStorageCluster.Equal(slaveStoragecluster),
				"unexpected slave cluster: %v", output.SlaveStorageCluster)
			assert.True(output.ZeroStorCluster.Equal(&zeroStorCluster),
				"unexpected zeroStor cluster: %v", output.ZeroStorCluster)
		case invalidKey := <-invalidKeyCh:
			assert.FailNow("received unexpected invalid key", "%v", invalidKey)
		}
	}

	testValue()

	source.SetTlogZeroStorCluster("a", "foocluster", nil)
	testInvalidKey(Key{ID: "foocluster", Type: KeyClusterZeroStor}) // error value should not be updated

	// now let's actually set the storage cluster
	source.SetZeroStorCluster("foocluster", &zeroStorCluster)
	testValue()

	// now let's set template cluster
	slaveStoragecluster = &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "231.201.201.200:2000"},
			StorageServerConfig{Address: "231.201.201.200:2020"},
		},
	}
	source.SetSlaveStorageCluster("a", "slave", slaveStoragecluster)
	testValue()

	// do another stupid illegal action
	source.SetTlogZeroStorCluster("a", "zeroStorCluster", nil)
	// trigger reload
	testInvalidKey(Key{ID: "zeroStorCluster", Type: KeyClusterZeroStor}) // error value should not be updated

	zeroStorCluster.Servers[0].Address = "3.3.3.3:33"
	source.SetTlogZeroStorCluster("a", "zeroStorCluster", &zeroStorCluster)
	testValue() // updating a storage cluster should be ok

	slaveStoragecluster = nil
	source.SetSlaveStorageCluster("a", "", slaveStoragecluster)
	testValue() // and slave storage cluster can even be dereferenced

	// when updating a cluster, which makes the cluster not valid for the used vdisk,
	// it should not apply the update either
	zeroStorCluster.Servers[0].Address = ""
	source.SetTlogZeroStorCluster("a", "zeroStorCluster", &zeroStorCluster)
	testInvalidKey(Key{ID: "zeroStorCluster", Type: KeyClusterZeroStor}) // no update should happen

	// updating a cluster in a valid way should still be possible
	zeroStorCluster.Servers = []ServerConfig{
		ServerConfig{"4.4.4.4:44"},
		ServerConfig{"5.5.5.5:55"},
	}
	source.SetTlogZeroStorCluster("a", "zeroStorCluster", &zeroStorCluster)
	testValue() // updating a storage cluster should be ok

	// cancel context
	cancel()
	// channel should be now closed
	select {
	case <-time.After(time.Millisecond * 500):
		assert.FailNow("timed out, ch doesn't seem to close")
	case _, open := <-ch:
		if !assert.False(open) {
			assert.FailNow("channel should have been closed")
		}
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
