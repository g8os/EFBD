package zerodisk

import (
	"errors"
	"fmt"
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

	// file config has a default value, in case no value was given
	if data == "" {
		data = defaultFileResource
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
	if info.ResourceType != FileConfigResource && info.Resource == nil {
		return ErrNoConfigResource
	}

	return nil
}

// CreateSource creates a source based on the config
func (info *ConfigInfo) CreateSource() (config.Source, error) {
	if info.ResourceType == FileConfigResource {
		path, err := fileResource(info.Resource)
		if err != nil {
			return nil, fmt.Errorf("can't create config source: %v", err)
		}

		return config.FileSource(path)
	}

	endpoints, err := etcdResource(info.Resource)
	if err != nil {
		return nil, fmt.Errorf("can't create config source: %v", err)
	}

	return config.ETCDV3Source(endpoints)
}

// String implements Stringer.String
func (info *ConfigInfo) String() string {
	if info.ResourceType == FileConfigResource {
		str, _ := fileResource(info.Resource)
		return str
	}

	// for all other resource type value we'll assume it's the etcd resource
	str, _ := etcdResourceToString(info.Resource)
	return str
}

// Set implements flag.Value.Set
func (info *ConfigInfo) Set(value string) error {
	parsedInfo, err := ParseConfigInfo(value)
	if err != nil {
		return err
	}

	info.Resource = parsedInfo.Resource
	info.ResourceType = parsedInfo.ResourceType
	return nil
}

// Type implements pflag.Value.Type
func (info *ConfigInfo) Type() string {
	return "ConfigInfo"
}

// ConfigResourceType specifies the type of config resource
// to retrieve the configuration from.
type ConfigResourceType uint8

const (
	// FileConfigResource defines the file config resource
	// This is the default config resource,
	// as the file config is the only config resource with
	// a valid default resource.
	FileConfigResource ConfigResourceType = 0
	// ETCDConfigResource defines the etcd config resource
	ETCDConfigResource ConfigResourceType = 1
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
	return "ConfigResourceType"
}

// Used to parse a etcd resource from a raw string.
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

// Used to interpolate a list of endpoints from any kind of accepted value.
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

// Used to interpolate a file path from any kind of accepted value.
func fileResource(value interface{}) (string, error) {
	if path, ok := value.(string); ok {
		if path == "" {
			return defaultFileResource, nil
		}

		return path, nil
	}

	if value == nil {
		return defaultFileResource, nil
	}

	return "", fmt.Errorf(
		"file config info: %v is not a valid file path", value)
}

// Used to turn any valid etcd resource value into a formatted string.
func etcdResourceToString(value interface{}) (string, error) {
	if endpoints, ok := value.([]string); ok {
		return strings.Join(endpoints, endpointsResourceSeperator), nil
	}

	if endpoint, ok := value.(string); ok {
		return endpoint, nil
	}

	return "", fmt.Errorf(
		"etcd config info: '%v' is not a valid etcd resource", value)
}

const (
	endpointsResourceSeperator = ","
	defaultFileResource        = "config.yml"
)
