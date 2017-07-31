package config

import (
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
	TemplateVdiskID        string
}

// Validate the optional properties of this config,
// using the VdiskType information.
func (cfg *NBDStorageConfig) Validate(storageType StorageType) error {
	// validate primary storage cluster
	err := cfg.StorageCluster.Validate()
	if err != nil {
		return fmt.Errorf(
			"invalid NBDStorageConfig, invalid primary storage cluster: %v", err)
	}

	// ensure that if optional template info is is given that the
	// required template info is also given
	if cfg.TemplateVdiskID != "" && cfg.TemplateStorageCluster == nil {
		return fmt.Errorf(
			"invalid NBDStorageConfig: 'TemplateVdiskID' is defined (%s)"+
				"while 'TemplateStorageCluster' is <nil>",
			cfg.TemplateVdiskID)
	}

	// otherwise ensire that if the template cluster is given, it is valid
	if cfg.TemplateStorageCluster != nil {
		err = cfg.TemplateStorageCluster.Validate()
		if err != nil {
			return fmt.Errorf(
				"invalid NBDStorageConfig, invalid template storage cluster: %v", err)
		}
	}

	// both deduped and semideduped storage types require
	// a metadata server to be defined
	if storageType != StorageNonDeduped &&
		cfg.StorageCluster.MetadataStorage == nil {
		return fmt.Errorf(
			"invalid NBDStorageConfig: storage type %s requires a storage server for metadata",
			storageType)
	}

	// composed config is valid
	return nil
}

// TlogStorageConfig contains all information needed
// to store tlogserver-related (meta)data.
type TlogStorageConfig struct {
	StorageCluster      StorageClusterConfig
	SlaveStorageCluster *StorageClusterConfig
}

// Validate the optional properties of this config,
// using the VdiskType information.
func (cfg *TlogStorageConfig) Validate(storageType StorageType) error {
	// validate primary storage cluster
	err := cfg.StorageCluster.Validate()
	if err != nil {
		return fmt.Errorf(
			"invalid TlogStorageConfig, invalid tlog storage cluster: %v", err)
	}

	if cfg.SlaveStorageCluster != nil {
		// ensure it is valid
		err = cfg.SlaveStorageCluster.Validate()
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
