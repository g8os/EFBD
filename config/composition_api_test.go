package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadNBDStorageConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadNBDStorageConfig(nil, "", nil)
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := NewStubSource()

	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(fileFormatVdiskConfig))
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid config")

	source.SetVdiskConfig("a", newValidStubVdiskConfig(VdiskTypeBoot, "mycluster"))
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to nil reference")

	source.SetStorageCluster("mycluster", new(StorageClusterConfig))
	_, err = ReadNBDStorageConfig(source, "a", nil)
	assert.Error(err, "should trigger error due to invalid reference")

	source.SetStorageCluster("mycluster", newValidStubStorageClusterConfig())
	nbdStorageCfg, err := ReadNBDStorageConfig(source, "a", nil)
	if assert.NoError(err, "should be fine as both the vdisk and storage are properly configured") {
		assert.Subset(
			[]StorageServerConfig{StorageServerConfig{Address: "localhost:16379"}},
			nbdStorageCfg.StorageCluster.DataStorage)
		if assert.NotNil(nbdStorageCfg.StorageCluster.MetadataStorage) {
			assert.Equal(
				StorageServerConfig{Address: "localhost:16379"},
				*nbdStorageCfg.StorageCluster.MetadataStorage)
		}
	}
}
