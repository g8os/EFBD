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

	_, err := ReadNBDStorageConfig(nil, "", nil)
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

	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey(Key{ID: "a", Type: KeyVdiskNBD})

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default
	source.SetPrimaryStorageCluster("a", "mycluster", nil)
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil reference")
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterStorage})

	source.SetStorageCluster("mycluster", new(StorageClusterConfig))
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid reference")
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterStorage})

	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage:     []StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
		MetadataStorage: &StorageServerConfig{Address: "localhost:16379"},
	})
	nbdStorageCfg, err := ReadNBDStorageConfig(source, "a", nil)
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(
			[]StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
			nbdStorageCfg.StorageCluster.DataStorage)
		if assert.NotNil(nbdStorageCfg.StorageCluster.MetadataStorage) {
			assert.Equal(
				StorageServerConfig{Address: "localhost:16379"},
				*nbdStorageCfg.StorageCluster.MetadataStorage)
		}
	}

	source.SetTemplateStorageCluster("a", "templateCluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16380"},
			StorageServerConfig{Address: "localhost:16381"},
			StorageServerConfig{Address: "localhost:16382"},
		},
	})
	nbdStorageCfg, err = ReadNBDStorageConfig(source, "a", nil)
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(
			[]StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
			nbdStorageCfg.StorageCluster.DataStorage)
		if assert.NotNil(nbdStorageCfg.StorageCluster.MetadataStorage) {
			assert.Equal(
				StorageServerConfig{Address: "localhost:16379"},
				*nbdStorageCfg.StorageCluster.MetadataStorage)
		}
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
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil slave cluster")
	testInvalidKey(Key{ID: "slaveCluster", Type: KeyClusterStorage})

	source.SetSlaveStorageCluster("a", "slaveCluster", new(StorageClusterConfig))
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid slave cluster")
	testInvalidKey(Key{ID: "slaveCluster", Type: KeyClusterStorage})

	source.SetSlaveStorageCluster("a", "slaveCluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
			StorageServerConfig{Address: "localhost:16380"},
		},
	})
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid slave cluster (missing metadata)")
	testInvalidKey(Key{ID: "slaveCluster", Type: KeyClusterStorage})

	source.SetSlaveStorageCluster("a", "slaveCluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
			StorageServerConfig{Address: "localhost:16380"},
		},
		MetadataStorage: &StorageServerConfig{Address: "localhost:16379"},
	})

	nbdStorageCfg, err = ReadNBDStorageConfig(source, "a", nil)
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(
			[]StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
			nbdStorageCfg.StorageCluster.DataStorage)
		if assert.NotNil(nbdStorageCfg.StorageCluster.MetadataStorage) {
			assert.Equal(
				StorageServerConfig{Address: "localhost:16379"},
				*nbdStorageCfg.StorageCluster.MetadataStorage)
		}
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
			if assert.NotNil(nbdStorageCfg.SlaveStorageCluster.MetadataStorage) {
				assert.Equal(
					StorageServerConfig{Address: "localhost:16379"},
					*nbdStorageCfg.SlaveStorageCluster.MetadataStorage)
			}
		}
	}
}

func TestReadTlogStorageConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadTlogStorageConfig(nil, "", nil)
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

	_, err = ReadTlogStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadTlogStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey(Key{ID: "a", Type: KeyVdiskTlog})

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default

	source.SetTlogZeroStorCluster("a", "mycluster", new(ZeroStorClusterConfig))
	_, err = ReadTlogStorageConfig(source, "a", nil)
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
		Servers:         []Server{Server{"1.1.1.1:11"}},
		MetadataServers: []Server{Server{"2.2.2.2:22"}},
	}
	source.SetTlogZeroStorCluster("a", "mycluster", &originalZeroStorCfg)
	tlogStorCfg, err := ReadTlogStorageConfig(source, "a", nil)
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(originalZeroStorCfg, tlogStorCfg.ZeroStorCluster)
	}

	// add nil slave cluster
	source.SetSlaveStorageCluster("a", "slaveCluster", nil)

	_, err = ReadTlogStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil slave cluster")
	testInvalidKey(Key{ID: "slaveCluster", Type: KeyClusterStorage})

	// add invalid slave cluster
	slaveClusterCfg := StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:3000"},
			StorageServerConfig{Address: "localhost:3002", Database: 42},
		},
	}
	source.SetSlaveStorageCluster("a", "slaveCluster", &slaveClusterCfg)

	_, err = ReadTlogStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid slave cluster")
	testInvalidKey(Key{ID: "slaveCluster", Type: KeyClusterStorage})

	// add valid slave cluster
	slaveClusterCfg.MetadataStorage = &StorageServerConfig{Address: "localhost:2030"}
	source.SetSlaveStorageCluster("a", "slaveCluster", &slaveClusterCfg)

	tlogStorCfg, err = ReadTlogStorageConfig(source, "a", nil)
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

	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	})
	_, err = WatchNBDStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing metadata storage")
	testInvalidKey(Key{ID: "mycluster", Type: KeyClusterStorage})

	// last one is golden
	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
		MetadataStorage: &StorageServerConfig{Address: "localhost:16379"},
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
		MetadataStorage: &StorageServerConfig{Address: "localhost:16379"},
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
	assert.Equal(
		StorageServerConfig{Address: "localhost:16379"}, *output.StorageCluster.MetadataStorage)
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
		MetadataStorage: &StorageServerConfig{Address: "localhost:16381"},
	})

	// trigger reload (even though it was broken before)
	output = <-ch
	assert.Nil(output.TemplateStorageCluster)
	assert.Equal(
		StorageServerConfig{Address: "localhost:16381"}, *output.StorageCluster.MetadataStorage)
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
		MetadataStorage: &StorageServerConfig{Address: "localhost:16379"},
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
		MetadataStorage: &StorageServerConfig{Address: "localhost:16381"},
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
		MetadataStorage: &StorageServerConfig{Address: "123.123.13.13:40"},
	}
	source.SetTemplateStorageCluster("a", "master", templateStoragecluster)
	testValue()

	// metadata server is not required for template storage
	templateStoragecluster.MetadataStorage = nil
	source.SetStorageCluster("master", templateStoragecluster)
	testValue()

	// but can be given, and it will be ignored by any user
	templateStoragecluster.MetadataStorage = &StorageServerConfig{Address: "localhost:300"}
	source.SetStorageCluster("master", templateStoragecluster)
	testValue()

	// now let's set slave cluster
	slaveStoragecluster = &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "slave:200"},
			StorageServerConfig{Address: "slave:201"},
		},
		MetadataStorage: &StorageServerConfig{Address: "slave:200", Database: 42},
	}
	source.SetSlaveStorageCluster("a", "slave", slaveStoragecluster)
	testValue()

	// metadata server is required for slave storage
	slaveStoragecluster.MetadataStorage = nil
	source.SetStorageCluster("slave", slaveStoragecluster)
	testInvalidKey(Key{ID: "slave", Type: KeyClusterStorage}) // error value should not be updated

	// even after, error, we can set a good value
	slaveStoragecluster.MetadataStorage = &StorageServerConfig{Address: "localhost:300"}
	source.SetStorageCluster("slave", slaveStoragecluster)
	testValue()

	// do another stupid illegal action
	source.SetPrimaryStorageCluster("a", "primary", nil)
	// trigger reload
	testInvalidKey(Key{ID: "primary", Type: KeyClusterStorage}) // error value should not be updated

	primaryStorageCluster.MetadataStorage.Database = 3
	source.SetStorageCluster("primary", primaryStorageCluster)
	testValue() // updating a storage cluster should be ok

	templateStoragecluster = nil
	source.SetTemplateStorageCluster("a", "", templateStoragecluster)
	testValue() // and template storage cluster can even be dereferenced

	// when updating a cluster, which makes the cluster not valid for the used vdisk,
	// it should not apply the update either
	primaryStorageCluster.MetadataStorage = nil
	source.SetStorageCluster("primary", primaryStorageCluster)
	testInvalidKey(Key{ID: "primary", Type: KeyClusterStorage}) // no update should happen

	// updating a cluster in a valid way should still be possible
	primaryStorageCluster.MetadataStorage = &StorageServerConfig{
		Address: "localhost:16379", Database: 2,
	}
	primaryStorageCluster.DataStorage = append(primaryStorageCluster.DataStorage,
		StorageServerConfig{Address: "localhost:2000", Database: 5})
	source.SetStorageCluster("primary", primaryStorageCluster)
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
		Servers:         []Server{Server{"1.1.1.1:11"}},
		MetadataServers: []Server{Server{"2.2.2.2:22"}},
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
		Servers:         []Server{Server{"1.1.1.1:11"}},
		MetadataServers: []Server{Server{"2.2.2.2:22"}},
	})
	ch, err := WatchTlogStorageConfig(ctx, source, "a")
	assert.NoError(err, "should be valid")

	output := <-ch
	assert.Nil(output.SlaveStorageCluster)
	if assert.Len(output.ZeroStorCluster.Servers, 1) {
		assert.Equal(
			Server{"1.1.1.1:11"},
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
		Servers:         []Server{Server{"3.3.3.3:33"}},
		MetadataServers: []Server{Server{"2.2.2.2:22"}},
	})

	// trigger reload (even though it was broken before)
	output = <-ch
	assert.Nil(output.SlaveStorageCluster)
	if assert.Len(output.ZeroStorCluster.Servers, 1) {
		assert.Equal(
			Server{"3.3.3.3:33"},
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
		Servers:         []Server{Server{"1.1.1.1:11"}},
		MetadataServers: []Server{Server{"2.2.2.2:22"}},
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
		MetadataStorage: &StorageServerConfig{Address: "123.123.13.13:40"},
	}
	source.SetSlaveStorageCluster("a", "slave", slaveStoragecluster)
	testValue()

	// setting an invalid slave Storage cluster should not be possible
	slaveStoragecluster.MetadataStorage = nil
	source.SetStorageCluster("slave", slaveStoragecluster)
	testInvalidKey(Key{ID: "slave", Type: KeyClusterStorage}) // error value should not be updated

	slaveStoragecluster.MetadataStorage = &StorageServerConfig{Address: "localhost:300"}
	source.SetStorageCluster("slave", slaveStoragecluster)
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
	zeroStorCluster.Servers = []Server{
		Server{"4.4.4.4:44"},
		Server{"5.5.5.5:55"},
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
