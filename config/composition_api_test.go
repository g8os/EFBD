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

	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid config")

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default
	source.SetPrimaryStorageCluster("a", "mycluster", nil)
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil reference")

	source.SetStorageCluster("mycluster", new(StorageClusterConfig))
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid reference")

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

	_, err = ReadTlogStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadTlogStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid config")

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default
	source.SetTlogStorageCluster("a", "mycluster", nil)
	_, err = ReadTlogStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil reference")

	source.SetStorageCluster("mycluster", new(StorageClusterConfig))
	_, err = ReadTlogStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid reference")

	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
	})
	nbdStorageCfg, err := ReadTlogStorageConfig(source, "a", nil)
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(
			[]StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
			nbdStorageCfg.StorageCluster.DataStorage)
		assert.Nil(nbdStorageCfg.StorageCluster.MetadataStorage)
	}

	source.SetSlaveStorageCluster("a", "slavecluster", &StorageClusterConfig{
		DataStorage:     []StorageServerConfig{StorageServerConfig{Address: "localhost:16380"}},
		MetadataStorage: &StorageServerConfig{Address: "localhost:16381"},
	})

	nbdStorageCfg, err = ReadTlogStorageConfig(source, "a", nil)
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Equal(
			[]StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
			nbdStorageCfg.StorageCluster.DataStorage)
		assert.Nil(nbdStorageCfg.StorageCluster.MetadataStorage)
		if assert.NotNil(nbdStorageCfg.SlaveStorageCluster) {
			assert.Equal(
				[]StorageServerConfig{StorageServerConfig{Address: "localhost:16380"}},
				nbdStorageCfg.SlaveStorageCluster.DataStorage)
			if assert.NotNil(nbdStorageCfg.SlaveStorageCluster.MetadataStorage) {
				assert.Equal(
					StorageServerConfig{Address: "localhost:16381"},
					*nbdStorageCfg.SlaveStorageCluster.MetadataStorage)
			}
		}
	}
}

func TestWatchNBDStorageConfig_FailAtStartup(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := WatchNBDStorageConfig(ctx, nil, "a")
	assert.Equal(err, ErrNilSource)

	source := NewStubSource()

	_, err = WatchNBDStorageConfig(ctx, source, "")
	assert.Equal(err, ErrNilID)

	_, err = WatchNBDStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing configs")

	source.SetPrimaryStorageCluster("a", "mycluster", nil)
	_, err = WatchNBDStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing/nil configs")

	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	})
	_, err = WatchNBDStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing metadata storage")

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
	assert.NoError(err, "should be valid")

	output := <-ch
	assert.Nil(output.TemplateStorageCluster)
	assert.Equal(
		StorageServerConfig{Address: "localhost:16379"}, *output.StorageCluster.MetadataStorage)
	if assert.Len(output.StorageCluster.DataStorage, 1) {
		assert.Equal(
			StorageServerConfig{Address: "localhost:16379"},
			output.StorageCluster.DataStorage[0])
	}

	testTimeout := func() {
		select {
		case <-time.After(time.Millisecond * 500):
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		}
	}

	// now let's break it
	source.SetStorageCluster("mycluster", nil)
	testTimeout()

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

	testValue := func() {
		output := <-ch
		assert.True(output.StorageCluster.Equal(primaryStorageCluster),
			"unexpected primary cluster: %v", output.StorageCluster)
		assert.True(output.TemplateStorageCluster.Equal(templateStoragecluster),
			"unexpected template cluster: %v", output.TemplateStorageCluster)
		assert.True(output.SlaveStorageCluster.Equal(slaveStoragecluster),
			"unexpected slave cluster: %v", output.SlaveStorageCluster)
	}

	testValue()

	testTimeout := func() {
		select {
		case <-time.After(time.Millisecond * 500):
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		}
	}

	source.SetPrimaryStorageCluster("a", "foocluster", nil)
	// trigger reload
	testTimeout() // error value should not be updated

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
	testTimeout() // error value should not be updated

	// even after, error, we can set a good value
	slaveStoragecluster.MetadataStorage = &StorageServerConfig{Address: "localhost:300"}
	source.SetStorageCluster("slave", slaveStoragecluster)
	testValue()

	// do another stupid illegal action
	source.SetPrimaryStorageCluster("a", "foocluster", nil)
	// trigger reload
	testTimeout() // error value should not be updated

	primaryStorageCluster.MetadataStorage.Database = 3
	source.SetStorageCluster("foocluster", primaryStorageCluster)
	testValue() // updating a storage cluster should be ok

	templateStoragecluster = nil
	source.SetTemplateStorageCluster("a", "", templateStoragecluster)
	testValue() // and template storage cluster can even be dereferenced

	// when updating a cluster, which makes the cluster not valid for the used vdisk,
	// it should not apply the update either
	primaryStorageCluster.MetadataStorage = nil
	source.SetStorageCluster("foocluster", primaryStorageCluster)
	testTimeout() // no update should happen

	// updating a cluster in a valid way should still be possible
	primaryStorageCluster.MetadataStorage = &StorageServerConfig{
		Address: "localhost:16379", Database: 2,
	}
	primaryStorageCluster.DataStorage = append(primaryStorageCluster.DataStorage,
		StorageServerConfig{Address: "localhost:2000", Database: 5})
	source.SetStorageCluster("foocluster", primaryStorageCluster)
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

	_, err = WatchTlogStorageConfig(ctx, source, "")
	assert.Equal(err, ErrNilID)

	_, err = WatchTlogStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing configs")

	source.SetTlogStorageCluster("a", "mycluster", nil)
	_, err = WatchTlogStorageConfig(ctx, source, "a")
	assert.Error(err, "should trigger error due to missing/nil configs")

	// last one is golden
	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	})
	_, err = WatchTlogStorageConfig(ctx, source, "a")
	assert.NoError(err, "should be fine now")
}

func TestWatchTlogStorageConfig_FailAfterSuccess(t *testing.T) {
	assert := assert.New(t)

	source := NewStubSource()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source.SetTlogStorageCluster("a", "mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	})
	ch, err := WatchTlogStorageConfig(ctx, source, "a")
	assert.NoError(err, "should be valid")

	output := <-ch
	assert.Nil(output.SlaveStorageCluster)
	assert.Nil(output.StorageCluster.MetadataStorage)
	if assert.Len(output.StorageCluster.DataStorage, 1) {
		assert.Equal(
			StorageServerConfig{Address: "localhost:16379"},
			output.StorageCluster.DataStorage[0])
	}

	testTimeout := func() {
		select {
		case <-time.After(time.Millisecond * 500):
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		}
	}

	// now let's break it
	source.SetStorageCluster("mycluster", nil)
	testTimeout()

	// now let's fix it again
	source.SetStorageCluster("mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16380"},
		},
	})

	// trigger reload (even though it was broken before)
	output = <-ch
	assert.Nil(output.SlaveStorageCluster)
	assert.Nil(output.StorageCluster.MetadataStorage)
	if assert.Len(output.StorageCluster.DataStorage, 1) {
		assert.Equal(
			StorageServerConfig{Address: "localhost:16380"},
			output.StorageCluster.DataStorage[0])
	}
}

func TestWatchTlogStorageConfig_ChangeClusterReference(t *testing.T) {
	assert := assert.New(t)

	source := NewStubSource()

	tlogStorageCluster := &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
			StorageServerConfig{Address: "localhost:16380"},
		},
	}

	var slaveStoragecluster *StorageClusterConfig

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source.SetTlogStorageCluster("a", "mycluster", tlogStorageCluster)
	ch, err := WatchTlogStorageConfig(ctx, source, "a")
	if !assert.NoError(err, "should be valid") {
		return
	}

	testValue := func() {
		output := <-ch
		assert.True(output.StorageCluster.Equal(tlogStorageCluster),
			"unexpected tlog cluster: %v", output.StorageCluster)
		assert.True(output.SlaveStorageCluster.Equal(slaveStoragecluster),
			"unexpected slave cluster: %v", output.SlaveStorageCluster)
	}

	testValue()

	testTimeout := func() {
		select {
		case <-time.After(time.Millisecond * 500):
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		}
	}

	source.SetTlogStorageCluster("a", "foocluster", nil)
	// trigger reload
	testTimeout() // error value should not be updated

	tlogStorageCluster = &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16381"},
		},
	}

	// now let's actually set the storage cluster
	source.SetTlogStorageCluster("a", "foocluster", tlogStorageCluster)
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
	testTimeout() // error value should not be updated

	slaveStoragecluster.MetadataStorage = &StorageServerConfig{Address: "localhost:300"}
	source.SetStorageCluster("slave", slaveStoragecluster)
	testValue()

	// do another stupid illegal action
	source.SetTlogStorageCluster("a", "foocluster", nil)
	// trigger reload
	testTimeout() // error value should not be updated

	tlogStorageCluster.DataStorage[0].Database = 3
	source.SetStorageCluster("foocluster", tlogStorageCluster)
	testValue() // updating a storage cluster should be ok

	slaveStoragecluster = nil
	source.SetSlaveStorageCluster("a", "", slaveStoragecluster)
	testValue() // and slave storage cluster can even be dereferenced

	// when updating a cluster, which makes the cluster not valid for the used vdisk,
	// it should not apply the update either
	tlogStorageCluster.DataStorage = nil
	source.SetStorageCluster("foocluster", tlogStorageCluster)
	testTimeout() // no update should happen

	// updating a cluster in a valid way should still be possible
	tlogStorageCluster.DataStorage = []StorageServerConfig{
		StorageServerConfig{Address: "localhost:16379", Database: 2},
	}
	tlogStorageCluster.DataStorage = append(tlogStorageCluster.DataStorage,
		StorageServerConfig{Address: "localhost:2000", Database: 5})
	source.SetStorageCluster("foocluster", tlogStorageCluster)
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
