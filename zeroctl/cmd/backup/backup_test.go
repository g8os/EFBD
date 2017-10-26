package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"
)

func TestStorageConfigToString(t *testing.T) {
	assert := assert.New(t)

	// invalid configs should return empty string
	for _, invalidConfig := range invalidStorageConfigs {
		assert.Empty(invalidConfig.String())
	}

	// valid configs should return the string we expect
	for _, validConfig := range validStorageConfigs {
		assert.Equal(validConfig.String, validConfig.Config.String())
	}
}

func TestStorageConfigFromString(t *testing.T) {
	assert := assert.New(t)

	// config can be created from nil string
	cfg, err := newStorageConfig("")
	if assert.NoError(err) {
		assert.Equal(backup.DefaultLocalRoot, cfg.String())
	}

	// valid cases
	for _, validConfig := range validStorageConfigs {
		resource := validConfig.String
		if validConfig.Config.StorageType == localStorageType {
			resource = "file://" + resource // as our directory doesn't exist (perhaps)
		}

		cfg, err := newStorageConfig(resource)
		if !assert.NoError(err) {
			continue
		}

		str := cfg.String()
		assert.Equal(validConfig.String, str)

		cfg = storageConfig{StorageType: storageType(42)}
		if !assert.NoError(cfg.Set(resource)) {
			continue
		}

		str = cfg.String()
		assert.Equal(validConfig.String, str)
	}
}

var validStorageConfigs = []struct {
	Config storageConfig
	String string
}{
	// Nil Storage Config is valid (is Local storage with default path)
	{storageConfig{}, backup.DefaultLocalRoot},
	// Same as above, but more explicit
	{storageConfig{
		StorageType: localStorageType,
	}, backup.DefaultLocalRoot},
	// Same as above, but even more expliti
	{storageConfig{
		StorageType: localStorageType,
		Resource:    backup.DefaultLocalRoot,
	}, backup.DefaultLocalRoot},
	// a simple ftp server config
	{storageConfig{
		StorageType: ftpStorageType,
		Resource: backup.FTPServerConfig{
			Address: "localhost:21",
		},
	}, "ftp://localhost:21"},
	// A User FTP Storage Example
	{storageConfig{
		StorageType: ftpStorageType,
		Resource: backup.FTPServerConfig{
			Address:  "localhost:2000",
			RootDir:  "/bar",
			Username: "foo",
		},
	}, "ftp://foo@localhost:2000/bar"},
	// A Secure User FTP Storage Example
	{storageConfig{
		StorageType: ftpStorageType,
		Resource: backup.FTPServerConfig{
			Address:  "localhost:2000",
			RootDir:  "/bar",
			Username: "foo",
			Password: "boo",
		},
	}, "ftp://foo:boo@localhost:2000/bar"},
}

var invalidStorageConfigs = []storageConfig{
	// nil Resource is invalid for FTP Server
	storageConfig{
		StorageType: ftpStorageType,
	},
	// invalid config Type
	storageConfig{
		StorageType: storageType(42),
	},
	// invalid Resource
	storageConfig{
		Resource: 42,
	},
	// Invalid Storage type
	storageConfig{
		StorageType: storageType(42),
		Resource:    42,
	},
	// valid FTP url,
	// but the Resource is expected to be a FTPStorageConfig
	storageConfig{
		StorageType: ftpStorageType,
		Resource:    "ftp://localhost:21",
	},
	storageConfig{
		StorageType: ftpStorageType,
		Resource: backup.FTPServerConfig{
			Address: ".foo",
		},
	},
}
