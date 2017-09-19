package config

import (
	"errors"
	"fmt"
)

// composition.go
// defines composed config files,
// which are not created directly from YAML data,
// but instead composed from a multitude of different data packages.

// NBDStorageConfig contains all information needed
// to store and use the (meta)data of a vdisk.
type NBDStorageConfig struct {
	StorageCluster         StorageClusterConfig
	TemplateStorageCluster *StorageClusterConfig
	SlaveStorageCluster    *StorageClusterConfig
}

// Validate all properties of this config,
// using the Storage Type information for the optional properties.
func (cfg *NBDStorageConfig) Validate(storageType StorageType) error {
	if cfg == nil {
		return nil
	}

	// validate primary storage cluster
	err := cfg.StorageCluster.Validate()
	if err != nil {
		return fmt.Errorf(
			"invalid NBDStorageConfig, invalid primary storage cluster: %v", err)
	}
	err = cfg.StorageCluster.ValidateStorageType(storageType)
	if err != nil {
		return fmt.Errorf("invalid NBDStorageConfig.PrimaryStorage: %v", err)
	}

	// ensure that if the template cluster is given, it is valid
	if cfg.TemplateStorageCluster != nil {
		err = cfg.TemplateStorageCluster.Validate()
		if err != nil {
			return fmt.Errorf(
				"invalid NBDStorageConfig, invalid template storage cluster: %v", err)
		}
	}

	// ensure that if the slave cluster is given, it is valid
	if cfg.SlaveStorageCluster != nil {
		// ensure the static struct validation for the slave config checks out
		err = cfg.SlaveStorageCluster.Validate()
		if err != nil {
			return fmt.Errorf(
				"invalid NBDStorageConfig, invalid slave storage cluster: %v", err)
		}

		// ensure that the slave cluster defines enough data (storage) servers
		slaveDataShardCount := len(cfg.SlaveStorageCluster.DataStorage)
		primaryDataShardCount := len(cfg.StorageCluster.DataStorage)
		if slaveDataShardCount < primaryDataShardCount {
			return errInsufficientSlaveDataShards
		}

		// ensure that the slave contains a metadata (storage) server if required
		err = cfg.SlaveStorageCluster.ValidateStorageType(storageType)
		if err != nil {
			return fmt.Errorf("invalid NBDStorageConfig.SlaveStorage: %v", err)
		}
	}

	return nil
}

var (
	errInsufficientSlaveDataShards = errors.New("invalid NBDStorageConfig.SlaveStorage: insufficient slave data storage servers" +
		" (require at least as much as the primary cluster has defined)")
)

// ValidateOptional validates the optional properties of this config,
// using the Storage Type information.
func (cfg *NBDStorageConfig) ValidateOptional(storageType StorageType) error {
	// both deduped and semideduped storage types require
	// a metadata server to be defined
	if storageType == StorageNonDeduped {
		return nil
	}

	return cfg.ValidateRequiredMetadataStorage()
}

// ValidateRequiredMetadataStorage allows you to ensure that the (slave) storage cluster
// defines a valid Metadata Storage.
func (cfg *NBDStorageConfig) ValidateRequiredMetadataStorage() error {
	if cfg == nil {
		return nil
	}

	if cfg.StorageCluster.MetadataStorage == nil {
		return errors.New("invalid NBDStorageConfig: require a storage server for (primary) metadata")
	}

	if cfg.SlaveStorageCluster != nil && cfg.SlaveStorageCluster.MetadataStorage == nil {
		return errors.New("invalid NBDStorageConfig: require a storage server for (slave) metadata")
	}

	// composed config is valid
	return nil
}

// Clone this config
func (cfg *NBDStorageConfig) Clone() NBDStorageConfig {
	var clone NBDStorageConfig
	if cfg == nil {
		return clone
	}

	clone.StorageCluster = cfg.StorageCluster.Clone()

	if cfg.TemplateStorageCluster != nil {
		templateClone := cfg.TemplateStorageCluster.Clone()
		clone.TemplateStorageCluster = &templateClone
	}

	if cfg.SlaveStorageCluster != nil {
		slaveClone := cfg.SlaveStorageCluster.Clone()
		clone.SlaveStorageCluster = &slaveClone
	}

	return clone
}

// TlogStorageConfig contains all information needed
// to store tlogserver-related (meta)data.
type TlogStorageConfig struct {
	ZeroStorCluster     ZeroStorClusterConfig
	SlaveStorageCluster *StorageClusterConfig
}

// Validate the required properties of this config,
// using the VdiskType information.
func (cfg *TlogStorageConfig) Validate(storageType StorageType) error {
	if cfg == nil {
		return nil
	}

	// validate primary storage cluster
	err := cfg.ZeroStorCluster.Validate()
	if err != nil {
		return fmt.Errorf(
			"invalid TlogStorageConfig, invalid 0-stor cluster: %v", err)
	}

	return cfg.ValidateOptional(storageType)
}

// ValidateOptional validates the optional properties of this config,
// using the Storage Type information.
func (cfg *TlogStorageConfig) ValidateOptional(storageType StorageType) error {
	// both deduped and semideduped storage types require
	// a metadata server to be defined for the slave cluster,
	// if one is given
	if cfg.SlaveStorageCluster != nil {
		// ensure it is valid
		err := cfg.SlaveStorageCluster.Validate()
		if err != nil {
			return fmt.Errorf(
				"invalid TlogStorageConfig, invalid slave storage cluster: %v", err)
		}

		// both deduped and semideduped storage types require
		// a metadata server to be defined
		if storageType != StorageNonDeduped &&
			cfg.SlaveStorageCluster.MetadataStorage == nil {
			return fmt.Errorf(
				"invalid TlogStorageConfig: storage type %s requires a storage server for slave metadata",
				storageType)
		}
	}

	// composed config is valid
	return nil
}
