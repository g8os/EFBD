package zerodisk

import (
	"fmt"

	"github.com/zero-os/0-Disk/config"
)

// ReadNBDVdisksConfig returns the requested NBDVdisksConfig
// from a given config resource.
func ReadNBDVdisksConfig(info ConfigInfo, serverID string) (*config.NBDVdisksConfig, error) {
	source, err := info.CreateSource()
	if err != nil {
		return nil, fmt.Errorf("ReadNBDVdisksConfig failed: %v", err)
	}
	defer source.Close()

	return config.ReadNBDVdisksConfig(source, serverID)
}

// ReadVdiskStaticConfig returns the requested VdiskStaticConfig
// from a given config resource.
func ReadVdiskStaticConfig(info ConfigInfo, vdiskID string) (*config.VdiskStaticConfig, error) {
	source, err := info.CreateSource()
	if err != nil {
		return nil, fmt.Errorf("ReadVdiskStaticConfig failed: %v", err)
	}
	defer source.Close()

	return config.ReadVdiskStaticConfig(source, vdiskID)
}

// ReadNBDStorageConfig returns the requested NBDStorageConfig
// from a given config resource.
func ReadNBDStorageConfig(info ConfigInfo, vdiskID string) (*config.NBDStorageConfig, error) {
	source, err := info.CreateSource()
	if err != nil {
		return nil, fmt.Errorf("ReadNBDStorageConfig failed: %v", err)
	}
	defer source.Close()

	return config.ReadNBDStorageConfig(source, vdiskID)
}

// ReadTlogStorageConfig returns the requested NBDStorageConfig
// from a given config resource.
func ReadTlogStorageConfig(info ConfigInfo, vdiskID string) (*config.TlogStorageConfig, error) {
	source, err := info.CreateSource()
	if err != nil {
		return nil, fmt.Errorf("ReadTlogStorageConfig failed: %v", err)
	}
	defer source.Close()

	return config.ReadTlogStorageConfig(source, vdiskID)
}

// ReadTlogClusterConfig returns the requested TlogClusterConfig
// from a given config resource.
func ReadTlogClusterConfig(info ConfigInfo, clusterID string) (*config.TlogClusterConfig, error) {
	source, err := info.CreateSource()
	if err != nil {
		return nil, fmt.Errorf("ReadStorageClusterConfig failed: %v", err)
	}
	defer source.Close()

	return config.ReadTlogClusterConfig(source, clusterID)
}
