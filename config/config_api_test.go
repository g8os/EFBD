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

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(id string) {
		expected := Key{ID: id, Type: KeyNBDServerVdisks}
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = ReadNBDVdisksConfig(source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadNBDVdisksConfig(source, "foo")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("foo")

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default
	source.SetPrimaryStorageCluster("a", "mycluster", &StorageClusterConfig{
		Servers: []StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
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

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(id string) {
		expected := Key{ID: id, Type: KeyVdiskStatic}
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = ReadVdiskStaticConfig(source, "a")
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = ReadVdiskStaticConfig(source, "a")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("a")

	source.SetVdiskConfig("a", nil) // delete it first, so we can properly create it by default
	// add vdisk by default as deduped (boot)
	source.SetPrimaryStorageCluster("a", "mycluster", &StorageClusterConfig{
		Servers: []StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
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

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(id string) {
		expected := Key{ID: id, Type: KeyClusterStorage}
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = ReadStorageClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetStorageCluster("foo", new(StorageClusterConfig))
	_, err = ReadStorageClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("foo")

	inputCfg := StorageClusterConfig{
		Servers: []StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
	}
	source.SetStorageCluster("foo", &inputCfg)

	outputCfg, err := ReadStorageClusterConfig(source, "foo")
	if assert.NoError(err, "should be ok") {
		assert.Equal(inputCfg, *outputCfg)
	}

	_, err = ReadStorageClusterConfig(source, "bar")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("bar")

	inputCfg = StorageClusterConfig{
		Servers: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
			StorageServerConfig{Address: "localhost:16379", Database: 42},
		},
	}
	source.SetStorageCluster("bar", &inputCfg)

	outputCfg, err = ReadStorageClusterConfig(source, "bar")
	if assert.NoError(err, "should be ok") {
		assert.Equal(inputCfg, *outputCfg)
	}
}

func TestReadZeroStoreClusterConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadZeroStoreClusterConfig(nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(id string) {
		expected := Key{ID: id, Type: KeyClusterZeroStor}
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = ReadZeroStoreClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetTlogZeroStorCluster("foo", "bar", new(ZeroStorClusterConfig))
	_, err = ReadZeroStoreClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("foo")

	inputCfg := ZeroStorClusterConfig{
		IYO: IYOCredentials{
			Org:       "foo org",
			Namespace: "foo namespace",
			ClientID:  "foo client",
			Secret:    "foo secret",
		},
		MetadataServers: []ServerConfig{
			ServerConfig{Address: "2.2.2.2:22"},
		},
		DataServers: []ServerConfig{
			ServerConfig{Address: "1.1.1.1:11"},
			ServerConfig{Address: "1.1.1.1:22"},
		},
		DataShards:   1,
		ParityShards: 1,
	}
	source.SetTlogZeroStorCluster("foo", "bar", &inputCfg)

	outputCfg, err := ReadZeroStoreClusterConfig(source, "bar")
	if assert.NoError(err, "should be ok") {
		assert.Equal(inputCfg, *outputCfg)
	}

	_, err = ReadZeroStoreClusterConfig(source, "lorem")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("lorem")
}

func TestReadTlogClusterConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadTlogClusterConfig(nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(id string) {
		expected := Key{ID: id, Type: KeyClusterTlog}
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = ReadTlogClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetTlogCluster("foo", new(TlogClusterConfig))
	_, err = ReadTlogClusterConfig(source, "foo")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("foo")

	inputCfg := TlogClusterConfig{
		Servers: []string{
			"localhost:2001",
			"localhost:2002",
		},
	}
	source.SetTlogCluster("foo", &inputCfg)

	outputCfg, err := ReadTlogClusterConfig(source, "foo")
	if assert.NoError(err, "should be ok") {
		assert.Equal(inputCfg, *outputCfg)
	}

	_, err = ReadTlogClusterConfig(source, "bar")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("bar")
}

func TestWatchTlogClusterConfig(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := WatchTlogClusterConfig(ctx, nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(id string) {
		expected := Key{ID: id, Type: KeyClusterTlog}
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = WatchTlogClusterConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetTlogCluster("foo", new(TlogClusterConfig))
	_, err = WatchTlogClusterConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("foo")

	inputCfg := TlogClusterConfig{
		Servers: []string{
			"localhost:2001",
			"localhost:2002",
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

	testValue(inputCfg)

	// add one server
	inputCfg.Servers = append(inputCfg.Servers, "localhost:16379")
	source.SetTlogCluster("foo", &inputCfg)
	testValue(inputCfg)

	// delete cluster
	source.SetTlogCluster("foo", nil)
	// now no config should be send, as the new config is invalid
	testInvalidKey("foo")

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

func TestWatchZeroStorClusterConfig(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := WatchZeroStorClusterConfig(ctx, nil, "bar")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(id string) {
		expected := Key{ID: id, Type: KeyClusterZeroStor}
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = WatchZeroStorClusterConfig(ctx, source, "bar")
	assert.Error(err, "should trigger error due to nil config")

	source.SetTlogZeroStorCluster("foo", "bar", new(ZeroStorClusterConfig))
	_, err = WatchZeroStorClusterConfig(ctx, source, "bar")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("bar")

	inputCfg := ZeroStorClusterConfig{
		IYO: IYOCredentials{
			Org:       "foo org",
			Namespace: "foo namespace",
			ClientID:  "foo client",
			Secret:    "foo secret",
		},
		MetadataServers: []ServerConfig{
			ServerConfig{Address: "3.3.3.3:33"},
		},
		DataServers: []ServerConfig{
			ServerConfig{Address: "1.1.1.1:11"},
			ServerConfig{Address: "2.2.2.2:22"},
		},
		DataShards:   1,
		ParityShards: 1,
	}
	source.SetTlogZeroStorCluster("foo", "bar", &inputCfg)

	ch, err := WatchZeroStorClusterConfig(ctx, source, "bar")
	if !assert.NoError(err) {
		return
	}

	testValue := func(cfg ZeroStorClusterConfig) {
		output := <-ch
		if !assert.Equal(cfg, output) {
			assert.FailNow("invalid returned value")
		}
	}

	testValue(inputCfg)

	// make invalid, this should make it mark the key as invalid
	var emptyZeroStor ZeroStorClusterConfig
	source.SetTlogZeroStorCluster("foo", "bar", &emptyZeroStor)
	testInvalidKey("bar")

	// delete cluster
	source.SetTlogZeroStorCluster("foo", "bar", nil)
	// this should make it also Invalid
	testInvalidKey("bar")

	// make it valid again
	source.SetTlogZeroStorCluster("foo", "bar", &inputCfg)
	testValue(inputCfg)

	// update the 0-stor configuration in a valid way
	inputCfg.IYO.ClientID = "boo client"
	inputCfg.DataServers = append(inputCfg.DataServers, ServerConfig{Address: "3.3.3.3:33"})
	inputCfg.DataShards++
	inputCfg.MetadataServers[0].Address = "4.4.4.4:33"

	source.SetTlogZeroStorCluster("foo", "bar", &inputCfg)
	testValue(inputCfg)

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

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(id string) {
		expected := Key{ID: id, Type: KeyClusterStorage}
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = WatchStorageClusterConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetStorageCluster("foo", new(StorageClusterConfig))
	_, err = WatchStorageClusterConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("foo")

	inputCfg := StorageClusterConfig{
		Servers: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
			StorageServerConfig{Address: "localhost:16379", Database: 42},
		},
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

	testValue(inputCfg)

	// delete one Data Storage Server
	inputCfg.Servers = inputCfg.Servers[0:1]
	source.SetStorageCluster("foo", &inputCfg)
	testValue(inputCfg)

	// make invalid, this should make it mark the key as invalid
	inputCfg.Servers = nil
	source.SetStorageCluster("foo", &inputCfg)
	testInvalidKey("foo")

	// delete cluster
	source.SetStorageCluster("foo", nil)
	// this should make it also Invalid
	testInvalidKey("foo")

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

	invalidKeyCh := source.InvalidKey()
	testInvalidKey := func(id string) {
		expected := Key{ID: id, Type: KeyNBDServerVdisks}
		select {
		case invalidKey := <-invalidKeyCh:
			if !assert.Equal(expected, invalidKey) {
				assert.FailNow("unexpected invalid key", "%v", invalidKey)
			}
		case <-time.After(time.Second):
			assert.FailNow("timed out while waiting for invalid key", "%v", expected)
		}
	}

	_, err = WatchNBDVdisksConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(VdiskStaticConfig))
	_, err = WatchNBDVdisksConfig(ctx, source, "foo")
	assert.Error(err, "should trigger error due to invalid config")
	testInvalidKey("foo")

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

	testValue([]string{"a", "b"})

	// add another vdisk ID
	source.SetVdiskConfig("c", &VdiskStaticConfig{
		BlockSize: 512,
		Size:      1,
		Type:      VdiskTypeDB,
	})
	testValue([]string{"a", "b", "c"})

	// delete 2 vdisks
	source.SetVdiskConfig("a", nil)
	testValue([]string{"b", "c"})
	source.SetVdiskConfig("b", nil)
	testValue([]string{"c"})

	// make invalid, this should make it timeout
	source.SetVdiskConfig("c", nil)
	testInvalidKey("foo")

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
