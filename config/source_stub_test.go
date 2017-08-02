package config

func newValidStubVdiskConfig(vtype VdiskType, clusterID string) *fileFormatVdiskConfig {
	return &fileFormatVdiskConfig{
		BlockSize: 4096,
		Size:      10,
		VdiskType: vtype,
		ReadOnly:  false,
		NBD: &VdiskNBDConfig{
			StorageClusterID: clusterID,
		},
	}
}

func newValidStubStorageClusterConfig() *StorageClusterConfig {
	return &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
		MetadataStorage: &StorageServerConfig{
			Address: "localhost:16379",
		},
	}
}
