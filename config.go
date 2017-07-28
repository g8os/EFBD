package zerodisk

import (
	"context"
	"errors"
	"strings"

	"github.com/zero-os/0-Disk/config"
)

// ConfigInfo is used to define which resource type to read from
// and how we can identify the resource itself.
type ConfigInfo struct {
	// ID of the Vdisk we want to read the config from.
	VdiskID string
	// Identifies the config resource.
	// ResourceType = file -> Resource defines config path.
	// ResourceType = etcd -> Resource defines comma seperated list of etcd endpoints.
	Resource string
	// Defines the type of config resource to be read from.
	ResourceType ConfigResourceType
}

// Validate this ConfigInfo struct.
func (info *ConfigInfo) Validate() error {
	if info.VdiskID == "" {
		return ErrNoVdiskID
	}
	if info.Resource == "" {
		return ErrNoConfigResource
	}

	return nil
}

// ConfigResourceType specifies the type of config resource
// to retrieve the configuration from.
type ConfigResourceType uint8

const (
	// ETCDConfigResource defines the etcd config resource
	ETCDConfigResource ConfigResourceType = 0
	// FileConfigResource defines the file config resource
	FileConfigResource ConfigResourceType = 1
)

const (
	etcdConfigResourceString = "etcd"
	fileConfigResourceString = "file"
)

var (
	// ErrNoConfigResource is returned when for a given config function
	// a nil resource was given by the user (the empty string)
	ErrNoConfigResource = errors.New("no config resource specified")
	// ErrNoVdiskID is returned when for a given config function
	// a nil string was given as the vdiskID
	ErrNoVdiskID = errors.New("no vdisk ID specified")
)

// UInt8 returns the config type as an uint8 value
func (ct ConfigResourceType) UInt8() uint8 {
	if ct > FileConfigResource {
		ct = ETCDConfigResource // defaults to ETCD Config Resource
	}

	return uint8(ct)
}

// String returns the name of the config resourcetype
func (ct ConfigResourceType) String() string {
	if ct == FileConfigResource {
		return fileConfigResourceString
	}

	// default to etcd
	return etcdConfigResourceString
}

// Set allows you to set this Config Resource Type
// using a raw string. Options: {etcd, file}
func (ct *ConfigResourceType) Set(str string) error {
	if str == "" {
		return errors.New("no string was given")
	}

	switch str {
	case fileConfigResourceString:
		*ct = FileConfigResource
	case etcdConfigResourceString:
		*ct = ETCDConfigResource
	default:
		return errors.New(str + " is not a valid config resource type")
	}

	return nil
}

// Type returns the flag type for a ConfigResourceType.
func (ct *ConfigResourceType) Type() string {
	return "configResourceType"
}

// ReadBaseConfig reads a vdisk's base config from a given config resource
func ReadBaseConfig(info ConfigInfo) (*config.BaseConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}

	if info.ResourceType == FileConfigResource {
		return config.ReadBaseConfigFile(info.VdiskID, info.Resource)
	}

	endpoints := strings.Split(info.Resource, endpointsResourceSeperator)
	return config.ReadBaseConfigETCD(info.VdiskID, endpoints)
}

// ReadNBDConfig reads a vdisk's NBD config from a given config resource
func ReadNBDConfig(info ConfigInfo) (*config.BaseConfig, *config.NBDConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, nil, err
	}

	if info.ResourceType == FileConfigResource {
		return config.ReadNBDConfigFile(info.VdiskID, info.Resource)
	}

	endpoints := strings.Split(info.Resource, endpointsResourceSeperator)
	return config.ReadNBDConfigETCD(info.VdiskID, endpoints)
}

// WatchNBDConfig allows you to receive an initial NBD Config over a channel,
// and a new NBD Config every time its resource is updated.
func WatchNBDConfig(ctx context.Context, info ConfigInfo) (<-chan config.NBDConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}

	if info.ResourceType == FileConfigResource {
		return config.WatchNBDConfigFile(ctx, info.VdiskID, info.Resource)
	}

	endpoints := strings.Split(info.Resource, endpointsResourceSeperator)
	return config.WatchNBDConfigETCD(ctx, info.VdiskID, endpoints)
}

// ReadTlogConfig reads a vdisk's TLog config from a given config resource
func ReadTlogConfig(info ConfigInfo) (*config.TlogConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}

	if info.ResourceType == FileConfigResource {
		return config.ReadTlogConfigFile(info.VdiskID, info.Resource)
	}

	endpoints := strings.Split(info.Resource, endpointsResourceSeperator)
	return config.ReadTlogConfigETCD(info.VdiskID, endpoints)
}

// WatchTlogConfig allows you to receive an initial Tlog Config over a channel,
// and a new TLog Config every time its resource is updated.
func WatchTlogConfig(ctx context.Context, info ConfigInfo) (<-chan config.TlogConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}

	if info.ResourceType == FileConfigResource {
		return config.WatchTlogConfigFile(ctx, info.VdiskID, info.Resource)
	}

	endpoints := strings.Split(info.Resource, endpointsResourceSeperator)
	return config.WatchTlogConfigETCD(ctx, info.VdiskID, endpoints)
}

// ReadSlaveConfig reads a vdisk's slave config from a given config resource
func ReadSlaveConfig(info ConfigInfo) (*config.SlaveConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}

	if info.ResourceType == FileConfigResource {
		return config.ReadSlaveConfigFile(info.VdiskID, info.Resource)
	}

	endpoints := strings.Split(info.Resource, endpointsResourceSeperator)
	return config.ReadSlaveConfigETCD(info.VdiskID, endpoints)
}

// WatchSlaveConfig allows you to receive an initial slave Config over a channel,
// and a new slave Config every time its resource is updated.
func WatchSlaveConfig(ctx context.Context, info ConfigInfo) (<-chan config.SlaveConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}

	if info.ResourceType == FileConfigResource {
		return config.WatchSlaveConfigFile(ctx, info.VdiskID, info.Resource)
	}

	endpoints := strings.Split(info.Resource, endpointsResourceSeperator)
	return config.WatchSlaveConfigETCD(ctx, info.VdiskID, endpoints)
}

const (
	endpointsResourceSeperator = ","
)
