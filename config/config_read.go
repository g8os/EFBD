package config

// ReadNBDVdisksConfig returns the requested NBDVdisksConfig
// from a given config source.
func ReadNBDVdisksConfig(source Source, serverID string) (*NBDVdisksConfig, error) {
	bytes, err := readConfig(source, serverID, KeyNBDServerVdisks)
	if err != nil {
		return nil, err
	}

	return NewNBDVdisksConfig(bytes)
}

// ReadVdiskStaticConfig returns the requested VdiskStaticConfig
// from a given config source.
func ReadVdiskStaticConfig(source Source, vdiskID string) (*VdiskStaticConfig, error) {
	bytes, err := readConfig(source, vdiskID, KeyVdiskStatic)
	if err != nil {
		return nil, err
	}

	return NewVdiskStaticConfig(bytes)
}

// ReadVdiskNBDConfig returns the requested VdiskNBDConfig
// from a given config source.
func ReadVdiskNBDConfig(source Source, vdiskID string) (*VdiskNBDConfig, error) {
	bytes, err := readConfig(source, vdiskID, KeyVdiskNBD)
	if err != nil {
		return nil, err
	}

	return NewVdiskNBDConfig(bytes)
}

// ReadVdiskTlogConfig returns the requested VdiskTlogConfig
// from a given config source.
func ReadVdiskTlogConfig(source Source, vdiskID string) (*VdiskTlogConfig, error) {
	bytes, err := readConfig(source, vdiskID, KeyVdiskTlog)
	if err != nil {
		return nil, err
	}

	return NewVdiskTlogConfig(bytes)
}

// ReadStorageClusterConfig returns the requested StorageClusterConfig
// from a given config source.
func ReadStorageClusterConfig(source Source, clusterID string) (*StorageClusterConfig, error) {
	bytes, err := readConfig(source, clusterID, KeyClusterStorage)
	if err != nil {
		return nil, err
	}

	return NewStorageClusterConfig(bytes)
}

// ReadTlogClusterConfig returns the requested TlogClusterConfig
// from a given config source.
func ReadTlogClusterConfig(source Source, clusterID string) (*TlogClusterConfig, error) {
	bytes, err := readConfig(source, clusterID, KeyClusterTlog)
	if err != nil {
		return nil, err
	}

	return NewTlogClusterConfig(bytes)
}

func readConfig(source Source, id string, keyType KeyType) ([]byte, error) {
	if source == nil {
		return nil, ErrNilSource
	}

	return source.Get(Key{
		ID:   id,
		Type: keyType,
	})
}
