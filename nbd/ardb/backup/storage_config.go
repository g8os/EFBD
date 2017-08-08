package backup

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrNilResource is returned when for a given config function
	// a nil resource was given by the user.
	ErrNilResource = errors.New("invalid nil resource given")
)

// NewStorageConfig creates a new storage config by
// implicitly infering the storage type based on the given data,
// and based on it use the data as the storage's config resource.
func NewStorageConfig(data string) (cfg StorageConfig, err error) {
	// nil data gets turned into a default storage config,
	// using the default local root
	if data == "" {
		cfg.Resource = defaultLocalRoot
		return
	}

	// if a file protocol was specified,
	// give it priority and return the local storage config for it,
	// using the specified path.
	if strings.HasPrefix(data, "file://") {
		cfg.Resource = strings.TrimPrefix(data, "file://")
		return
	}

	// try to interpret it as an FTP server config
	ftpStorageConfig, err := NewFTPStorageConfig(data)
	if err == nil {
		cfg.Resource = ftpStorageConfig
		cfg.StorageType = FTPStorageType
		return
	}
	err = nil

	// check if the given data points to a valid path, as a last resort
	if exists, _ := localFileExists(data, true); exists {
		cfg.Resource = data
		return
	}

	// invalid data given, cannot create a config based on it
	err = fmt.Errorf("%v is an invalid storage config resource string", data)
	return
}

// StorageConfig is the configuration used to create
// a (backup) storage based on a given type and resource.
type StorageConfig struct {
	// The resource used to create the specific storage.
	// Type = local -> Resource defines root directory path.
	// Type = ftp -> Resource defines ftp configuration.
	Resource interface{}
	// Defines the type of (backup) storage to
	// write/read the backup to/from.
	StorageType StorageType
}

// validate this StorageConfig struct.
func (cfg *StorageConfig) validate() error {
	if cfg.StorageType != LocalStorageType && cfg.Resource == nil {
		return ErrNilResource
	}

	if cfg.StorageType == FTPStorageType {
		ftpStorageConfig, ok := cfg.Resource.(FTPStorageConfig)
		if !ok {
			return ErrInvalidConfig
		}
		return ftpStorageConfig.validate()
	}

	if cfg.StorageType != LocalStorageType {
		return ErrInvalidConfig
	}
	if cfg.Resource != nil {
		if _, ok := cfg.Resource.(string); !ok {
			return ErrInvalidConfig
		}
	}
	return nil
}

// String implements Stringer.String
func (cfg *StorageConfig) String() string {
	if cfg == nil {
		return defaultLocalRoot
	}

	if cfg.validate() != nil {
		return ""
	}

	// if local config,
	// use default root if no resource (see: rootdir) specified,
	// otherwise simply return the specified path.
	if cfg.StorageType == LocalStorageType {
		if cfg.Resource == nil || cfg.Resource == "" {
			return defaultLocalRoot
		}
		path, _ := cfg.Resource.(string)
		return path
	}

	// must be an FTP Storage Config, as the config is valid
	ftpConfig, _ := cfg.Resource.(FTPStorageConfig)
	// and return the ftp config as a string
	return ftpConfig.String()
}

// Set implements flag.Value.Set
func (cfg *StorageConfig) Set(value string) (err error) {
	*cfg, err = NewStorageConfig(value)
	return err
}

// Type implements pflag.Value.Type
func (cfg *StorageConfig) Type() string {
	return "StorageConfig"
}

// StorageType specifies the type of (backup) storage
// to export to or import from.
type StorageType uint8

const (
	// LocalStorageType defines a local file storage,
	// meaning a backup would be stored/loaded,
	// directly on/from the local file storage.
	// This is also the default (backup) storage.
	LocalStorageType StorageType = 0
	// FTPStorageType defines the FTP storage,
	// meaning a backup would be stored/loaded,
	// on/from an FTP Server.
	// This is the (backup) storage used in production.
	FTPStorageType StorageType = 1
)

const (
	localStorageTypeStr = "local"
	ftpStorageTypeStr   = "ftp"
)

// String returns the name of the Config Source Type
func (st StorageType) String() string {
	switch st {
	case LocalStorageType:
		return localStorageTypeStr
	case FTPStorageType:
		return ftpStorageTypeStr
	default:
		return ""
	}
}

// Set allows you to set this Storage Type
// using a raw string. Options: {ftp, local}
func (st *StorageType) Set(str string) error {
	switch strings.ToLower(str) {
	case localStorageTypeStr:
		*st = LocalStorageType
	case ftpStorageTypeStr:
		*st = FTPStorageType
	default:
		return errors.New(str + " is not a valid storage type")
	}

	return nil
}

// Type returns the flag type for a StorageType.
func (st *StorageType) Type() string {
	return "StorageType"
}
