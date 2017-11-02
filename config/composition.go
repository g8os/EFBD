package config

import (
	"github.com/zero-os/0-Disk/errors"
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
func (cfg *NBDStorageConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	// validate primary storage cluster
	err := cfg.StorageCluster.Validate()
	if err != nil {
		return errors.Wrap(err,
			"invalid NBDStorageConfig, invalid primary storage cluster")
	}

	// ensure that if the template cluster is given, it is valid
	if cfg.TemplateStorageCluster != nil {
		err = cfg.TemplateStorageCluster.Validate()
		if err != nil {
			return errors.Wrap(err,
				"invalid NBDStorageConfig, invalid template storage cluster")
		}
	}

	// ensure that if the slave cluster is given, it is valid
	if cfg.SlaveStorageCluster != nil {
		// ensure the static struct validation for the slave config checks out
		err = cfg.SlaveStorageCluster.Validate()
		if err != nil {
			return errors.Wrap(err,
				"invalid NBDStorageConfig, invalid slave storage cluster")
		}

		// ensure that the slave cluster defines enough data (storage) servers
		slaveDataShardCount := len(cfg.SlaveStorageCluster.Servers)
		primaryDataShardCount := len(cfg.StorageCluster.Servers)
		if slaveDataShardCount < primaryDataShardCount {
			return errors.WrapError(ErrInvalidConfig, errInsufficientSlaveDataShards)
		}
	}

	return nil
}

var (
	errInsufficientSlaveDataShards = errors.New("invalid NBDStorageConfig.SlaveStorage: insufficient slave data storage servers" +
		" (require at least as much as the primary cluster has defined)")
)

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
	ZeroStorCluster ZeroStorClusterConfig
}

// Validate the required properties of this config,
// using the VdiskType information.
func (cfg *TlogStorageConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	// validate primary storage cluster
	err := cfg.ZeroStorCluster.Validate()
	if err != nil {
		return errors.Wrap(err,
			"invalid TlogStorageConfig, invalid 0-stor cluster")
	}

	// all valid
	return nil
}
