package config

import "fmt"

// ReadNBDStorageConfig returns the composed NBDStorageConfig
// based on several subconfigs read from a given config source.
func ReadNBDStorageConfig(source Source, vdiskID string) (*NBDStorageConfig, error) {
	nbdConfig, err := ReadVdiskNBDConfig(source, vdiskID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadNBDStorageConfig failed, invalid VdiskNBDConfig: %v", err)
	}

	// Create NBD Storage config
	nbdStorageConfig := new(NBDStorageConfig)

	// Fetch Primary Storage Cluster Config
	storageClusterConfig, err := ReadStorageClusterConfig(
		source, nbdConfig.StorageClusterID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadNBDStorageConfig failed, invalid StorageClusterConfig: %v", err)
	}
	nbdStorageConfig.StorageCluster = *storageClusterConfig

	// if template storage ID is given, fetch it
	if nbdConfig.TemplateStorageClusterID != "" {
		nbdStorageConfig.TemplateVdiskID = nbdConfig.TemplateVdiskID

		// Fetch Template Storage Cluster Config
		nbdStorageConfig.TemplateStorageCluster, err = ReadStorageClusterConfig(
			source, nbdConfig.TemplateStorageClusterID)
		if err != nil {
			return nil, fmt.Errorf(
				"ReadNBDStorageConfig failed, invalid TemplateStorageClusterConfig: %v", err)
		}
	}

	// validate optional properties of NBD Storage Config
	// based on the storage type of this vdisk
	staticConfig, err := ReadVdiskStaticConfig(source, vdiskID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadNBDStorageConfig failed, invalid VdiskStaticConfig: %v", err)
	}
	err = nbdStorageConfig.ValidateOptional(staticConfig.Type.StorageType())
	if err != nil {
		return nil, fmt.Errorf("ReadNBDStorageConfig failed: %v", err)
	}

	return nbdStorageConfig, nil
}

// ReadTlogStorageConfig returns the composed TlogStorageConfig
// based on several subconfigs read from a given config source.
func ReadTlogStorageConfig(source Source, vdiskID string) (*TlogStorageConfig, error) {
	tlogConfig, err := ReadVdiskTlogConfig(source, vdiskID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadTlogStorageConfig failed, invalid VdiskTlogConfig: %v", err)
	}

	// Create NBD Storage config
	tlogStorageConfig := new(TlogStorageConfig)

	// Fetch Tlog Storage Cluster Config
	storageClusterConfig, err := ReadStorageClusterConfig(
		source, tlogConfig.StorageClusterID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadTlogStorageConfig failed, invalid StorageClusterConfig: %v", err)
	}
	tlogStorageConfig.StorageCluster = *storageClusterConfig

	// if slave storage ID is given, fetch it
	if tlogConfig.SlaveStorageClusterID != "" {
		tlogStorageConfig.SlaveStorageCluster, err = ReadStorageClusterConfig(
			source, tlogConfig.SlaveStorageClusterID)
		if err != nil {
			return nil, fmt.Errorf(
				"ReadTlogStorageConfig failed, invalid SlaveStorageCluster: %v", err)
		}

		// validate optional properties of NBD Storage Config
		// based on the storage type of this vdisk
		staticConfig, err := ReadVdiskStaticConfig(source, vdiskID)
		if err != nil {
			return nil, fmt.Errorf(
				"ReadTlogStorageConfig failed, invalid VdiskStaticConfig: %v", err)
		}
		err = tlogStorageConfig.ValidateOptional(staticConfig.Type.StorageType())
		if err != nil {
			return nil, fmt.Errorf("ReadTlogStorageConfig failed: %v", err)
		}
	}

	return tlogStorageConfig, nil
}
