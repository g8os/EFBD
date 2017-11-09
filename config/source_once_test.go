package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnceSource(t *testing.T) {
	require := require.New(t)

	// nil source should panic
	require.Panics(func() {
		NewOnceSource(nil)
	}, "Providing NewOnceSource with a nil source should cause a panic")

	// create stubsource with data
	vdiskID := "testVdisk"
	clusterID := "testCluster"
	originalSource := NewStubSource()
	clusterCfg := StorageClusterConfig{
		Servers: []StorageServerConfig{
			StorageServerConfig{
				Address: "1.2.3.4:5678",
			},
		},
	}
	originalSource.SetPrimaryStorageCluster(vdiskID, clusterID, &clusterCfg)

	// pass it into a onceSource
	source := NewOnceSource(originalSource)

	// fetch key
	cfgBS, err := source.Get(Key{ID: clusterID, Type: KeyClusterStorage})
	require.NoError(err)
	require.NotEmpty(cfgBS)

	clusterCfgFromSource, err := NewStorageClusterConfig(cfgBS)
	require.NoError(err)
	require.True(clusterCfg.Equal(*clusterCfgFromSource),
		"cluster config from source should be equal to original cluster config")

	// twice (check for cache fetch trough code coverage)
	cfgBS, err = source.Get(Key{ID: clusterID, Type: KeyClusterStorage})
	require.NoError(err)
	require.NotEmpty(cfgBS)

	clusterCfgFromSource, err = NewStorageClusterConfig(cfgBS)
	require.NoError(err)
	require.True(clusterCfg.Equal(*clusterCfgFromSource),
		"cluster config from source should be equal to original cluster config")

	// update config and check if config is still as before the update
	newCluster := StorageClusterConfig{
		Servers: []StorageServerConfig{
			StorageServerConfig{
				Address: "8.7.6.5:4321",
			},
		},
	}
	originalSource.SetPrimaryStorageCluster(vdiskID, clusterID, &newCluster)

	cfgBS, err = source.Get(Key{ID: clusterID, Type: KeyClusterStorage})
	require.NoError(err)
	require.NotEmpty(cfgBS)

	clusterCfgFromSource, err = NewStorageClusterConfig(cfgBS)
	require.NoError(err)
	require.True(clusterCfg.Equal(*clusterCfgFromSource),
		"cluster config from source should be equal to original cluster config")
	require.False(newCluster.Equal(*clusterCfgFromSource),
		"cluster config should not equal the updated cluster config")

	// get an invalid key ID
	_, err = source.Get(Key{ID: "an invalid ID", Type: KeyClusterStorage})
	require.Error(err)

	// try to watch
	require.Panics(func() {
		source.Watch(nil, Key{})
	}, "calling Watch on a onceSource should cause a panic")
}

func TestDifferentKeyTypeSameID(t *testing.T) {
	require := require.New(t)

	// create stubsource with data
	vdiskID := "testVdisk"
	clusterID := "testCluster"
	originalSource := NewStubSource()

	// create StaticVdiskConfig
	vdiskCfg := &VdiskStaticConfig{
		BlockSize: 4096,
		Size:      10,
		Type:      VdiskTypeDB,
	}
	originalSource.SetVdiskConfig(vdiskID, vdiskCfg)

	// create vdiskNBDConfig
	clusterCfg := &StorageClusterConfig{
		Servers: []StorageServerConfig{
			StorageServerConfig{
				Address: "1.2.3.4:5678",
			},
		},
	}
	originalSource.SetTemplateStorageCluster(vdiskID, clusterID, clusterCfg)

	// pass it into a onceSource
	source := NewOnceSource(originalSource)

	// test if different key types with same ID are cashed separately
	bsVdisk, err := source.Get(Key{ID: vdiskID, Type: KeyVdiskStatic})
	require.NoError(err)

	cacheCntBefore := len(source.(*onceSource).cache)

	bsNBD, err := source.Get(Key{ID: vdiskID, Type: KeyVdiskNBD})
	require.NoError(err)

	cacheCntAfter := len(source.(*onceSource).cache)
	require.True(cacheCntBefore < cacheCntAfter,
		"cache count should be increased after getting a different key type with same ID")
	require.NotEqual(bsVdisk, bsNBD,
		"Returned config from source with different key types should be different")
}
