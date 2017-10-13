package backup

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"
)

// see `init` and `parsePosArguments` for more information
// about the meaning of each config property.
var vdiskCmdCfg struct {
	VdiskID         string                 // required
	SourceConfig    config.SourceConfig    // optional
	SnapshotID      string                 // optional
	PrivateKey      backup.CryptoKey       // optional
	CompressionType backup.CompressionType // optional
	JobCount        int                    // optional
	Force           bool                   //optional

	BackupStorageConfig storageConfig // optional
}

var (
	// errNilResource is returned when for a given config function
	// a nil resource was given by the user.
	errNilResource = errors.New("invalid nil resource given")
)

// newStorageConfig creates a new storage config by
// implicitly infering the storage type based on the given data,
// and based on it use the data as the storage's config resource.
func newStorageConfig(data string) (cfg storageConfig, err error) {
	// nil data gets turned into a default storage config,
	// using the default local root
	if data == "" {
		cfg.Resource = backup.DefaultLocalRoot
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
	ftpStorageConfig, err := backup.NewFTPServerConfig(data)
	if err == nil {
		cfg.Resource = ftpStorageConfig
		cfg.StorageType = ftpStorageType
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

func localFileExists(path string, dir bool) (bool, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return false, err
	}

	if !dir && stat.IsDir() {
		return false, errors.New("unexpected directory: " + path)
	}
	if dir && !stat.IsDir() {
		return false, errors.New("unexpected file: " + path)
	}

	return true, nil
}

// storageConfig is the configuration used to create
// a (backup) storage based on a given type and resource.
type storageConfig struct {
	// The resource used to create the specific storage.
	// Type = local -> Resource defines root directory path.
	// Type = ftp -> Resource defines ftp configuration.
	Resource interface{}
	// Defines the type of (backup) storage to
	// write/read the backup to/from.
	StorageType storageType
}

// String implements Stringer.String
func (cfg *storageConfig) String() string {
	if cfg == nil {
		return backup.DefaultLocalRoot
	}

	// if local config,
	// use default root if no resource (see: rootdir) specified,
	// otherwise simply return the specified path.
	if cfg.StorageType == localStorageType {
		if cfg.Resource == nil || cfg.Resource == "" {
			return backup.DefaultLocalRoot
		}
		path, _ := cfg.Resource.(string)
		return path
	}

	// must be an FTP Storage Config, as the config is valid
	ftpConfig, _ := cfg.Resource.(backup.FTPServerConfig)
	// and return the ftp config as a string
	return ftpConfig.String()
}

// Set implements flag.Value.Set
func (cfg *storageConfig) Set(value string) (err error) {
	*cfg, err = newStorageConfig(value)
	return err
}

// Type implements pflag.Value.Type
func (cfg *storageConfig) Type() string {
	return "storageConfig"
}

// StorageType specifies the type of (backup) storage
// to export to or import from.
type storageType uint8

const (
	// localStorageType defines a local file storage,
	// meaning a backup would be stored/loaded,
	// directly on/from the local file storage.
	// This is also the default (backup) storage.
	localStorageType storageType = 0
	// ftpStorageType defines the FTP storage,
	// meaning a backup would be stored/loaded,
	// on/from an FTP Server.
	// This is the (backup) storage used in production.
	ftpStorageType storageType = 1
)
