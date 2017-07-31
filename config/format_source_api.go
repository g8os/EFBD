package config

// format_source_api.go
// defines the generic interface implemented
// by all source implementations (e.g. etcd)

//// Configs unique per NBDServer
//
// func ReadNBDVdisksConfig<SOURCE>(serverID string) (*NBDVdisksConfig, error)
// func WatchNBDVdisksConfig<SOURCE>(serverID string) (<-chan NBDVdisksConfig, error)

//// Configs unique per Vdisk
//
// func ReadStaticVdiskConfig<SOURCE>(vdiskID string) (*VdiskStaticConfig, error)
//
// func ReadNBDStorageConfig<SOURCE>(vdiskID string) (*NBDStorageConfig, error)
// func WatchNBDStorageConfig<SOURCE>(vdiskID string) (<-chan NBDStorageConfig, error)
//
// func ReadTlogClusterConfig<SOURCE>(vdiskID string) (*TlogClusterConfig, error)
// func WatchTlogClusterConfig<SOURCE>(vdiskID string) (<-chan TlogClusterConfig, error)
//
// func ReadTlogStorageConfig<SOURCE>(vdiskID string) (*TlogStorageConfig, error)
// func WatchTlogStorageConfig<SOURCE>(vdiskID string) (<-chan TlogStorageConfig, error)

//// Configs unique per Storage Cluster
//
// func ReadStorageClusterConfig<SOURCE>(clusterID string) (*StorageClusterConfig, error)
// func WatchStorageClusterConfig<SOURCE>(clusterID string) (<-chan StorageClusterConfig, error)

//// Configs unique per Tlog Server Cluster
//
// func ReadTlogClusterConfig<SOURCE>(clusterID string) (*TlogClusterConfig, error)
// func WatchTlogClusterConfig<SOURCE>(clusterID string) (<-chan TlogClusterConfig, error)
