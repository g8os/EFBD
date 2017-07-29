package zerodisk

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	valid "github.com/asaskevich/govalidator"
	"github.com/zero-os/0-Disk/config"
)

// ParseConfigInfo can be used to implicitly infer the resource type
// and parse the resource from the given data based on that.
func ParseConfigInfo(data string) (*ConfigInfo, error) {
	if endpoints, err := etcdResourceFromString(data); err == nil {
		return &ConfigInfo{
			Resource:     endpoints,
			ResourceType: ETCDConfigResource,
		}, nil
	}

	// we'll just assume it is a file path,
	// as there is no perfect way to validate if it's a file
	return &ConfigInfo{
		Resource:     data,
		ResourceType: FileConfigResource,
	}, nil
}

// ConfigInfo is used to define which resource type to read from
// and how we can identify the resource itself.
type ConfigInfo struct {
	// Identifies the config resource.
	// ResourceType = file -> Resource defines file path.
	// ResourceType = etcd -> Resource defines etcd endpoints.
	Resource interface{}
	// Defines the type of config resource to be read from.
	ResourceType ConfigResourceType
}

// Validate this ConfigInfo struct.
func (info *ConfigInfo) Validate() error {
	if info.Resource == "" {
		return ErrNoConfigResource
	}

	return nil
}

func etcdResourceFromString(data string) ([]string, error) {
	if strings.Contains(data, endpointsResourceSeperator) {
		addresses := strings.Split(data, endpointsResourceSeperator)
		for _, address := range addresses {
			address = strings.TrimSpace(address)
			if !valid.IsDialString(address) {
				return nil, fmt.Errorf(
					"etcd config info: '%s' is not a valid address", address)
			}
		}

		return addresses, nil
	}

	data = strings.TrimSpace(data)
	if valid.IsDialString(data) {
		return []string{data}, nil
	}

	return nil, fmt.Errorf(
		"etcd config info: '%s' is not a valid address", data)
}

func etcdResource(value interface{}) ([]string, error) {
	if endpoints, ok := value.([]string); ok {
		return endpoints, nil
	}

	if str, ok := value.(string); ok {
		return etcdResourceFromString(str)
	}

	return nil, fmt.Errorf(
		"etcd config info: '%v' is not a valid etcd resource", value)
}

func fileResource(value interface{}) (string, error) {
	if path, ok := value.(string); ok {
		return path, nil
	}

	return "", fmt.Errorf(
		"file config info: %v is not a valid file path", value)
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
func ReadBaseConfig(vdiskID string, info ConfigInfo) (*config.BaseConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}
	if vdiskID == "" {
		return nil, ErrNoVdiskID
	}

	if info.ResourceType == FileConfigResource {
		path, err := fileResource(info.Resource)
		if err != nil {
			return nil, err
		}
		return config.ReadBaseConfigFile(vdiskID, path)
	}

	endpoints, err := etcdResource(info.Resource)
	if err != nil {
		return nil, err
	}
	return config.ReadBaseConfigETCD(vdiskID, endpoints)
}

// ReadNBDConfig reads a vdisk's NBD config from a given config resource
func ReadNBDConfig(vdiskID string, info ConfigInfo) (*config.BaseConfig, *config.NBDConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, nil, err
	}
	if vdiskID == "" {
		return nil, nil, ErrNoVdiskID
	}

	if info.ResourceType == FileConfigResource {
		path, err := fileResource(info.Resource)
		if err != nil {
			return nil, nil, err
		}
		return config.ReadNBDConfigFile(vdiskID, path)
	}

	endpoints, err := etcdResource(info.Resource)
	if err != nil {
		return nil, nil, err
	}
	return config.ReadNBDConfigETCD(vdiskID, endpoints)
}

// WatchNBDConfig allows you to receive an initial NBD Config over a channel,
// and a new NBD Config every time its resource is updated.
func WatchNBDConfig(ctx context.Context, vdiskID string, info ConfigInfo) (<-chan config.NBDConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}
	if vdiskID == "" {
		return nil, ErrNoVdiskID
	}

	if info.ResourceType == FileConfigResource {
		path, err := fileResource(info.Resource)
		if err != nil {
			return nil, err
		}
		return config.WatchNBDConfigFile(ctx, vdiskID, path)
	}

	endpoints, err := etcdResource(info.Resource)
	if err != nil {
		return nil, err
	}
	return config.WatchNBDConfigETCD(ctx, vdiskID, endpoints)
}

// ReadTlogConfig reads a vdisk's TLog config from a given config resource
func ReadTlogConfig(vdiskID string, info ConfigInfo) (*config.TlogConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}
	if vdiskID == "" {
		return nil, ErrNoVdiskID
	}

	if info.ResourceType == FileConfigResource {
		path, err := fileResource(info.Resource)
		if err != nil {
			return nil, err
		}
		return config.ReadTlogConfigFile(vdiskID, path)
	}

	endpoints, err := etcdResource(info.Resource)
	if err != nil {
		return nil, err
	}
	return config.ReadTlogConfigETCD(vdiskID, endpoints)
}

// WatchTlogConfig allows you to receive an initial Tlog Config over a channel,
// and a new TLog Config every time its resource is updated.
func WatchTlogConfig(ctx context.Context, vdiskID string, info ConfigInfo) (<-chan config.TlogConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}
	if vdiskID == "" {
		return nil, ErrNoVdiskID
	}

	if info.ResourceType == FileConfigResource {
		path, err := fileResource(info.Resource)
		if err != nil {
			return nil, err
		}
		return config.WatchTlogConfigFile(ctx, vdiskID, path)
	}

	endpoints, err := etcdResource(info.Resource)
	if err != nil {
		return nil, err
	}
	return config.WatchTlogConfigETCD(ctx, vdiskID, endpoints)
}

// ReadSlaveConfig reads a vdisk's slave config from a given config resource
func ReadSlaveConfig(vdiskID string, info ConfigInfo) (*config.SlaveConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}
	if vdiskID == "" {
		return nil, ErrNoVdiskID
	}

	if info.ResourceType == FileConfigResource {
		path, err := fileResource(info.Resource)
		if err != nil {
			return nil, err
		}
		return config.ReadSlaveConfigFile(vdiskID, path)
	}

	endpoints, err := etcdResource(info.Resource)
	if err != nil {
		return nil, err
	}
	return config.ReadSlaveConfigETCD(vdiskID, endpoints)
}

// WatchSlaveConfig allows you to receive an initial slave Config over a channel,
// and a new slave Config every time its resource is updated.
func WatchSlaveConfig(ctx context.Context, vdiskID string, info ConfigInfo) (<-chan config.SlaveConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}
	if vdiskID == "" {
		return nil, ErrNoVdiskID
	}

	if info.ResourceType == FileConfigResource {
		path, err := fileResource(info.Resource)
		if err != nil {
			return nil, err
		}
		return config.WatchSlaveConfigFile(ctx, vdiskID, path)
	}

	endpoints, err := etcdResource(info.Resource)
	if err != nil {
		return nil, err
	}
	return config.WatchSlaveConfigETCD(ctx, vdiskID, endpoints)
}

// ReadVdisksConfig reads the vdisks config from a given config resource
func ReadVdisksConfig(info ConfigInfo) (*config.VdisksConfig, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}

	if info.ResourceType == FileConfigResource {
		path, err := fileResource(info.Resource)
		if err != nil {
			return nil, err
		}
		return config.ReadVdisksConfigFile(path)
	}

	endpoints, err := etcdResource(info.Resource)
	if err != nil {
		return nil, err
	}
	return config.ReadVdisksConfigETCD(endpoints)
}

// ParseStorageServerConfigString allows you to parse a raw dial config string.
// Dial Config String are a simple format used to specify ardb connection configs
// easily as a command line argument.
// The format is as follows: `<ip>:<port>[@<db_index>]`,
// where the db_index is optional.
// The parsing algorithm of this function is very forgiving,
// and returns an error only in case an invalid address is given.
func ParseStorageServerConfigString(dialConfigString string) (config config.StorageServerConfig, err error) {
	parts := strings.Split(dialConfigString, "@")
	if n := len(parts); n < 2 {
		config.Address = dialConfigString
	} else {
		config.Database, err = strconv.Atoi(parts[n-1])
		if err != nil {
			err = nil // ignore actual error
			n++       // not a valid database, thus probably part of address
		}
		// join any other parts back together,
		// if for some reason an @ sign makes part of the address
		config.Address = strings.Join(parts[:n-1], "@")
	}

	if !valid.IsDialString(config.Address) {
		err = fmt.Errorf("%s is not a valid storage address", config.Address)
	}

	return
}

// ParseCSStorageServerConfigStrings allows you to parse a slice of raw dial config strings.
// Dial Config Strings are a simple format used to specify ardb connection configs
// easily as a command line argument.
// The format is as follows: `<ip>:<port>[@<db_index>][,<ip>:<port>[@<db_index>]]`,
// where the db_index is optional, and you can give multiple configs by
// seperating them with a comma.
// The parsing algorithm of this function is very forgiving,
// and returns an error only in case an invalid address is given.
func ParseCSStorageServerConfigStrings(dialCSConfigString string) (configs []config.StorageServerConfig, err error) {
	if dialCSConfigString == "" {
		return nil, nil
	}
	dialConfigStrings := strings.Split(dialCSConfigString, ",")

	var cfg config.StorageServerConfig

	// convert all connection strings into ConnectionConfigs
	for _, dialConfigString := range dialConfigStrings {
		// remove whitespace around
		dialConfigString = strings.TrimSpace(dialConfigString)

		// trailing commas are allowed
		if dialConfigString == "" {
			continue
		}

		// parse one storage server config string
		cfg, err = ParseStorageServerConfigString(dialConfigString)
		if err != nil {
			configs = nil
			return
		}

		configs = append(configs, cfg)
	}

	return
}

const (
	endpointsResourceSeperator = ","
)
