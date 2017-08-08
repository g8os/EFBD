package backup

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidStorageConfigs(t *testing.T) {
	assert := assert.New(t)

	for _, validConfig := range validStorageConfigs {
		assert.NoErrorf(validConfig.Config.validate(), "%v", validConfig)
	}
}

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
	cfg, err := NewStorageConfig("")
	if assert.NoError(err) {
		assert.Equal(defaultLocalRoot, cfg.String())
	}

	// valid cases
	for _, validConfig := range validStorageConfigs {
		resource := validConfig.String
		if validConfig.Config.StorageType == LocalStorageType {
			resource = "file://" + resource // as our directory doesn't exist (perhaps)
		}

		cfg, err := NewStorageConfig(resource)
		if !assert.NoError(err) {
			continue
		}

		str := cfg.String()
		assert.Equal(validConfig.String, str)

		cfg = StorageConfig{StorageType: StorageType(42)}
		if !assert.NoError(cfg.Set(resource)) {
			continue
		}

		str = cfg.String()
		assert.Equal(validConfig.String, str)
	}
}

func TestInvalidStorageConfigs(t *testing.T) {
	assert := assert.New(t)

	for _, invalidConfig := range invalidStorageConfigs {
		assert.Errorf(invalidConfig.validate(), "%v", invalidConfig)
	}
}

var validStorageConfigs = []struct {
	Config StorageConfig
	String string
}{
	// Nil Storage Config is valid (is Local storage with default path)
	{StorageConfig{}, defaultLocalRoot},
	// Same as above, but more explicit
	{StorageConfig{
		StorageType: LocalStorageType,
	}, defaultLocalRoot},
	// Same as above, but even more expliti
	{StorageConfig{
		StorageType: LocalStorageType,
		Resource:    defaultLocalRoot,
	}, defaultLocalRoot},
	// a simple ftp server config
	{StorageConfig{
		StorageType: FTPStorageType,
		Resource: FTPStorageConfig{
			Address: "localhost:21",
		},
	}, "ftp://localhost:21"},
	// A User FTP Storage Example
	{StorageConfig{
		StorageType: FTPStorageType,
		Resource: FTPStorageConfig{
			Address:  "ftp://localhost:2000/bar",
			Username: "foo",
		},
	}, "ftp://foo@localhost:2000/bar"},
	// A Secure User FTP Storage Example
	{StorageConfig{
		StorageType: FTPStorageType,
		Resource: FTPStorageConfig{
			Address:  "ftp://localhost:2000/bar",
			Username: "foo",
			Password: "boo",
		},
	}, "ftp://foo:boo@localhost:2000/bar"},
}

var invalidStorageConfigs = []StorageConfig{
	// nil Resource is invalid for FTP Server
	StorageConfig{
		StorageType: FTPStorageType,
	},
	// invalid config Type
	StorageConfig{
		StorageType: StorageType(42),
	},
	// invalid Resource
	StorageConfig{
		Resource: 42,
	},
	// Invalid Storage type
	StorageConfig{
		StorageType: StorageType(42),
		Resource:    42,
	},
	// valid FTP url,
	// but the Resource is expected to be a FTPStorageConfig
	StorageConfig{
		StorageType: FTPStorageType,
		Resource:    "ftp://localhost:21",
	},
	StorageConfig{
		StorageType: FTPStorageType,
		Resource: FTPStorageConfig{
			Address: ".foo",
		},
	},
}

func TestStorageTypeToString(t *testing.T) {
	assert := assert.New(t)

	// each storage type should match its string version
	assert.Equal(localStorageTypeStr, LocalStorageType.String())
	assert.Equal(ftpStorageTypeStr, FTPStorageType.String())

	// all invalid values should return the empty string
	assert.Empty(StorageType(42).String())
}

func TestStorageTypeFromString(t *testing.T) {
	assert := assert.New(t)

	// the given string has to be valid
	var st StorageType
	assert.Error(st.Set(""))
	assert.Error(st.Set("foo"))

	st = StorageType(42)
	// setting the actual values should be fine
	if assert.NoError(st.Set(localStorageTypeStr)) {
		assert.Equal(LocalStorageType, st)
	}
	if assert.NoError(st.Set(ftpStorageTypeStr)) {
		assert.Equal(FTPStorageType, st)
	}

	// converting from a string is caps-insensitive
	if assert.NoError(st.Set(strings.ToUpper(localStorageTypeStr))) {
		assert.Equal(LocalStorageType, st)
	}
	if assert.NoError(st.Set(strings.ToUpper(ftpStorageTypeStr))) {
		assert.Equal(FTPStorageType, st)
	}
}
