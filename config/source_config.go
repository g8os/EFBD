package config

import (
	"errors"
	"fmt"
	"strings"

	valid "github.com/asaskevich/govalidator"
)

// NewSourceConfig creates a new source config by
// implicitly infering the source type based on the given data,
// and based on it use the data as the config's resource.
func NewSourceConfig(data string) (SourceConfig, error) {
	if endpoints, err := etcdResourceFromString(data); err == nil {
		return SourceConfig{
			Resource:   endpoints,
			SourceType: ETCDSourceType,
		}, nil
	}

	// file config has a default value, in case no value was given
	if data == "" {
		data = defaultFileResource
	}

	// we'll just assume it is a file path,
	// as there is no perfect way to validate if it's a file
	return SourceConfig{
		Resource:   data,
		SourceType: FileSourceType,
	}, nil
}

// SourceConfig is the configuration used to create
// a source based on a given type and resource.
type SourceConfig struct {
	// The resource used to identify the specific config origins.
	// Type = file -> Resource defines file path.
	// Type = etcd -> Resource defines etcd endpoints.
	Resource interface{}
	// Defines the type of source to be read from,
	// and thus also what type of Resource this is.
	SourceType SourceType
}

// Validate this SourceConfig struct.
func (cfg *SourceConfig) Validate() error {
	if cfg.SourceType != FileSourceType && cfg.Resource == nil {
		return ErrNilResource
	}

	return nil
}

// String implements Stringer.String
func (cfg *SourceConfig) String() string {
	if cfg == nil {
		return defaultFileResource
	}

	if cfg.SourceType == FileSourceType {
		str, _ := fileResource(cfg.Resource)
		return str
	}

	// for all other resource type value we'll assume it's the etcd source
	str, _ := etcdResourceToString(cfg.Resource)
	return str
}

// Set implements flag.Value.Set
func (cfg *SourceConfig) Set(value string) (err error) {
	*cfg, err = NewSourceConfig(value)
	return err
}

// Type implements pflag.Value.Type
func (cfg *SourceConfig) Type() string {
	return "SourceConfig"
}

// SourceType specifies the type of config source
// to retrieve the configuration from.
type SourceType uint8

const (
	// FileSourceType defines the file config resource
	// This is the default config resource,
	// as the file config is the only config resource with
	// a valid default resource.
	FileSourceType SourceType = 0
	// ETCDSourceType defines the etcd config resource
	ETCDSourceType SourceType = 1
)

const (
	fileSourceTypeString = "file"
	etcdSourceTypeString = "etcd"
)

// String returns the name of the Config Source Type
func (st SourceType) String() string {
	if st == FileSourceType {
		return fileSourceTypeString
	}

	// default to etcd
	return etcdSourceTypeString
}

// Set allows you to set this Config Source Type
// using a raw string. Options: {etcd, file}
func (st *SourceType) Set(str string) error {
	if str == "" {
		return errors.New("no string was given")
	}

	switch str {
	case fileSourceTypeString:
		*st = FileSourceType
	case etcdSourceTypeString:
		*st = ETCDSourceType
	default:
		return errors.New(str + " is not a valid config source type")
	}

	return nil
}

// Type returns the flag type for a SourceType.
func (st *SourceType) Type() string {
	return "SourceType"
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
