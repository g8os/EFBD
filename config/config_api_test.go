package config

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadNBDVdisksConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadNBDVdisksConfig(nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	_, err = ReadNBDVdisksConfig(source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadNBDVdisksConfig(source, "foo")
	assert.Error(err, "should trigger error due to invalid config")

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default
	source.SetPrimaryStorageCluster("a", "mycluster", &StorageClusterConfig{
		DataStorage:     []StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
		MetadataStorage: &StorageServerConfig{Address: "localhost:16379"},
	})
	vdisksCfg, err := ReadNBDVdisksConfig(source, "foo")
	if assert.NoError(err, "should return us 'a'") && assert.Len(vdisksCfg.Vdisks, 1) {
		assert.Equal("a", vdisksCfg.Vdisks[0])
	}

	source.SetPrimaryStorageCluster("b", "mycluster", nil)
	vdisksCfg, err = ReadNBDVdisksConfig(source, "foo")
	if assert.NoError(err, "should return us 'a, b'") && assert.Len(vdisksCfg.Vdisks, 2) {
		assert.Subset([]string{"a", "b"}, vdisksCfg.Vdisks)
	}
}

func TestReadVdiskStaticConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadVdiskStaticConfig(nil, "a")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	_, err = ReadVdiskStaticConfig(source, "a")
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadVdiskStaticConfig(source, "a")
	assert.Error(err, "should trigger error due to invalid config")

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default
	// add vdisk by default as deduped (boot)
	source.SetPrimaryStorageCluster("a", "mycluster", &StorageClusterConfig{
		DataStorage: []StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
	})

	_, err = ReadVdiskStaticConfig(source, "a")
	assert.NoError(err, "should be ok")

	// create a custom vdisk
	staticCfg := VdiskStaticConfig{
		BlockSize: 2048,
		Size:      42,
		Type:      VdiskTypeCache,
		ReadOnly:  true,
	}
	source.SetVdiskConfig("foo", &staticCfg)

	// read it
	vdiskStaticCfg, err := ReadVdiskStaticConfig(source, "foo")
	if assert.NoError(err, "should be ok") {
		assert.Equal(staticCfg, *vdiskStaticCfg)
	}
}

func TestReadStorageClusterConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadStorageClusterConfig(nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	_, err = ReadStorageClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetStorageCluster("foo", new(StorageClusterConfig))
	_, err = ReadStorageClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to invalid config")

	inputCfg := StorageClusterConfig{
		DataStorage: []StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
	}
	source.SetStorageCluster("foo", &inputCfg)

	outputCfg, err := ReadStorageClusterConfig(source, "foo")
	if assert.NoError(err, "should be ok") {
		assert.Equal(inputCfg, *outputCfg)
	}

	inputCfg = StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
			StorageServerConfig{Address: "localhost:16379", Database: 42},
		},
		MetadataStorage: &StorageServerConfig{Address: "localhost:16381", Database: 2},
	}
	source.SetStorageCluster("bar", &inputCfg)

	outputCfg, err = ReadStorageClusterConfig(source, "bar")
	if assert.NoError(err, "should be ok") {
		assert.Equal(inputCfg, *outputCfg)
	}
}

func TestReadTlogClusterConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadTlogClusterConfig(nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	_, err = ReadTlogClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetTlogCluster("foo", new(TlogClusterConfig))
	_, err = ReadTlogClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to invalid config")

	inputCfg := TlogClusterConfig{
		Servers: []TlogServerConfig{
			TlogServerConfig{Address: "localhost:2001"},
			TlogServerConfig{Address: "localhost:2002"},
		},
	}
	source.SetTlogCluster("foo", &inputCfg)

	outputCfg, err := ReadTlogClusterConfig(source, "foo")
	if assert.NoError(err, "should be ok") {
		assert.Equal(inputCfg, *outputCfg)
	}
}

func TestWatchTlogClusterConfig(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := WatchTlogClusterConfig(ctx, nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	_, err = WatchTlogClusterConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetTlogCluster("foo", new(TlogClusterConfig))
	_, err = WatchTlogClusterConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to invalid config")

	inputCfg := TlogClusterConfig{
		Servers: []TlogServerConfig{
			TlogServerConfig{Address: "localhost:2001"},
			TlogServerConfig{Address: "localhost:2002"},
		},
	}
	source.SetTlogCluster("foo", &inputCfg)

	ch, err := WatchTlogClusterConfig(ctx, source, "foo")
	if !assert.NoError(err) {
		return
	}

	testValue := func(cfg TlogClusterConfig) {
		output := <-ch
		if !assert.Equal(cfg, output) {
			assert.FailNow("invalid returned value")
		}
	}

	testTimeout := func() {
		select {
		case <-time.After(time.Second * 1):
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		}
	}

	testValue(inputCfg)

	// add one server
	inputCfg.Servers = append(inputCfg.Servers, TlogServerConfig{Address: "localhost:16379"})
	source.SetTlogCluster("foo", &inputCfg)
	source.TriggerReload()
	testValue(inputCfg)

	// delete cluster
	source.SetTlogCluster("foo", nil)
	source.TriggerReload()
	// now no config should be send, as the new config is invalid
	testTimeout()

	// cancel context
	cancel()
	// channel should be now closed
	select {
	case <-time.After(time.Second * 1):
		assert.FailNow("timed out, ch doesn't seem to close")
	case _, open := <-ch:
		if !assert.False(open) {
			assert.FailNow("channel should have been closed")
		}
	}
}

func TestWatchStorageClusterConfig(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := WatchStorageClusterConfig(ctx, nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	_, err = WatchStorageClusterConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetStorageCluster("foo", new(StorageClusterConfig))
	_, err = WatchStorageClusterConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to invalid config")

	inputCfg := StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
			StorageServerConfig{Address: "localhost:16379", Database: 42},
		},
		MetadataStorage: &StorageServerConfig{Address: "localhost:16381", Database: 2},
	}
	source.SetStorageCluster("foo", &inputCfg)

	ch, err := WatchStorageClusterConfig(ctx, source, "foo")
	if !assert.NoError(err) {
		return
	}

	testValue := func(cfg StorageClusterConfig) {
		output := <-ch
		if !assert.Equal(cfg, output) {
			assert.FailNow("invalid returned value")
		}
	}

	testTimeout := func() {
		select {
		case <-time.After(time.Second * 1):
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		}
	}

	testValue(inputCfg)

	// delete meta Data Storage Server
	inputCfg.MetadataStorage = nil
	source.SetStorageCluster("foo", &inputCfg)
	source.TriggerReload()
	testValue(inputCfg)

	// delete one Data Storage Server
	inputCfg.DataStorage = inputCfg.DataStorage[0:1]
	source.SetStorageCluster("foo", &inputCfg)
	source.TriggerReload()
	testValue(inputCfg)

	// make invalid, this should make it timeout
	inputCfg.DataStorage = nil
	source.SetStorageCluster("foo", &inputCfg)
	source.TriggerReload()
	testTimeout()

	// delete cluster
	source.SetStorageCluster("foo", nil)
	// now no config should be send, as the new config is invalid
	source.TriggerReload()
	testTimeout()

	// cancel context
	cancel()
	// channel should be now closed
	select {
	case <-time.After(time.Second * 1):
		assert.FailNow("timed out, ch doesn't seem to close")
	case _, open := <-ch:
		if !assert.False(open) {
			assert.FailNow("channel should have been closed")
		}
	}
}

func TestWatchNBDVdisksConfig(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := WatchNBDVdisksConfig(ctx, nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	_, err = WatchNBDVdisksConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = WatchNBDVdisksConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to invalid config")

	source.SetVdiskConfig("a", &VdiskStaticConfig{
		BlockSize: 4096,
		Size:      10,
		Type:      VdiskTypeCache,
	})

	source.SetVdiskConfig("b", &VdiskStaticConfig{
		BlockSize: 2048,
		Size:      5,
		Type:      VdiskTypeBoot,
	})

	ch, err := WatchNBDVdisksConfig(ctx, source, "foo")
	if !assert.NoError(err) {
		return
	}

	testValue := func(vdisks []string) {
		output := <-ch
		if !assert.Subset(vdisks, output.Vdisks) {
			assert.FailNow("invalid returned value")
		}
	}

	testTimeout := func() {
		select {
		case <-time.After(time.Second * 1):
		case output := <-ch:
			assert.FailNow("received unexpected value", "%v", output)
		}
	}

	testValue([]string{"a", "b"})

	// add another vdisk ID
	source.SetVdiskConfig("c", &VdiskStaticConfig{
		BlockSize: 512,
		Size:      1,
		Type:      VdiskTypeDB,
	})
	source.TriggerReload()
	testValue([]string{"a", "b", "c"})

	// delete 2 vdisks
	source.SetVdiskConfig("a", nil)
	source.SetVdiskConfig("b", nil)
	source.TriggerReload()
	testValue([]string{"c"})

	// make invalid, this should make it timeout
	source.SetVdiskConfig("c", nil)
	source.TriggerReload()
	testTimeout()

	// cancel context
	cancel()
	// channel should be now closed
	select {
	case <-time.After(time.Second * 1):
		assert.FailNow("timed out, ch doesn't seem to close")
	case _, open := <-ch:
		if !assert.False(open) {
			assert.FailNow("channel should have been closed")
		}
	}
}
