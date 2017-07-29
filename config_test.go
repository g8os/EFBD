package zerodisk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/config"
)

func TestNilConfigResourceType(t *testing.T) {
	var crt ConfigResourceType
	assert.Equal(t, ETCDConfigResource, crt)
}

func TestConfigResourceTypeInequality(t *testing.T) {
	assert.NotEqual(t, ETCDConfigResource, FileConfigResource)
}

func TestConfigResourceTypeToString(t *testing.T) {
	assert := assert.New(t)

	// valid config resource types
	assert.Equal(etcdConfigResourceString, ETCDConfigResource.String())
	assert.Equal(fileConfigResourceString, FileConfigResource.String())

	// anything else defaults to etcd as well
	assert.Equal(etcdConfigResourceString, ConfigResourceType(42).String())
}

func TestConfigResourceTypeFromString(t *testing.T) {
	assert := assert.New(t)

	var err error
	var crt ConfigResourceType

	// etcd config resource type
	err = crt.Set(etcdConfigResourceString)
	if assert.NoError(err) {
		assert.Equal(ETCDConfigResource, crt)
	}

	// file config resource type
	err = crt.Set(fileConfigResourceString)
	if assert.NoError(err) {
		assert.Equal(FileConfigResource, crt)
	}

	// any other string will result in an error
	err = crt.Set("foo")
	if assert.Error(err) {
		// and the variable will remain unchanged
		assert.Equal(FileConfigResource, crt)
	}
}

var validConfigPaths = []string{
	"config",
	"config ",
	"config.txt",
	"config.yml",
	"./config",
	"./config.yml",
	"~/config.yml",
	"../config.yml",
	"/cfgs/config",
	"../../config",
	"../../config.yml",
	"/home/user/config.yml",
}

var validETCDStrings = []string{
	"localhost:3000",
	"localhost:3000 ",
	"localhost:3000,localhost:3001",
	"localhost:3000, localhost:3001",
	"127.0.0.1:22",
	"127.0.0.1:22,localhost:3000",
	"foo:2000",
	" foo:2000",
	" foo:2000 ",
	"foo:2000,bar:3000",
	"[2001:0db8:0000:0000:0000:ff00:0042:8329]:42",
	"[2001:db8:0:0:0:ff00:42:8329]:1111",
	"[2001:db8::ff00:42:8329]:33 ",
}

func TestETCDResourceFromString(t *testing.T) {
	assert := assert.New(t)

	for _, validETCDString := range validETCDStrings {
		endpoints, err := etcdResourceFromString(validETCDString)
		if assert.NoError(err) {
			assert.True(len(endpoints) > 0)
		}
	}
}

func TestParseConfigInfo(t *testing.T) {
	assert := assert.New(t)

	for _, validETCDString := range validETCDStrings {
		info, err := ParseConfigInfo(validETCDString)
		if assert.NoError(err, validETCDString) && assert.NotNil(info, validETCDString) {
			assert.Equal(ETCDConfigResource, info.ResourceType, validETCDString)
			endpoints, ok := info.Resource.([]string)
			if assert.True(ok, validETCDString) {
				assert.True(len(endpoints) > 0, validETCDString)
			}
		}
	}

	for _, validPath := range validConfigPaths {
		info, err := ParseConfigInfo(validPath)
		if assert.NoError(err, validPath) && assert.NotNil(info, validPath) {
			assert.Equal(FileConfigResource, info.ResourceType, validPath)
			path, ok := info.Resource.(string)
			if assert.True(ok, validPath) {
				assert.Equal(validPath, path)
			}
		}
	}
}

func TestParseValidCSStorageServerConfigStrings(t *testing.T) {
	testCases := []struct {
		input    string
		expected []config.StorageServerConfig
	}{
		{"", nil},
		{",", nil},
		{",,,,", nil},
		{"0.0.0.0:1", scconfigs("0.0.0.0:1", 0)},
		{"0.0.0.0:1 ", scconfigs("0.0.0.0:1", 0)},
		{" 0.0.0.0:1", scconfigs("0.0.0.0:1", 0)},
		{" 0.0.0.0:1 ", scconfigs("0.0.0.0:1", 0)},
		{"0.0.0.0:1@0", scconfigs("0.0.0.0:1", 0)},
		{"0.0.0.0:1@1", scconfigs("0.0.0.0:1", 1)},
		{"0.0.0.0:1@1,", scconfigs("0.0.0.0:1", 1)},
		{"0.0.0.0:1@1,,,,", scconfigs("0.0.0.0:1", 1)},
		{"0.0.0.0:1,0.0.0.0:1", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 0)},
		{"0.0.0.0:1,0.0.0.0:1,", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 0)},
		{"0.0.0.0:1, 0.0.0.0:1", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 0)},
		{"0.0.0.0:1, 0.0.0.0:1 ", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 0)},
		{"0.0.0.0:1,0.0.0.0:1@1", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 1)},
		{"1.2.3.4:5@6,7.8.9.10:11@12", scconfigs("1.2.3.4:5", 6, "7.8.9.10:11", 12)},
	}

	for _, testCase := range testCases {
		serverConfigs, err := ParseCSStorageServerConfigStrings(testCase.input)
		if assert.Nil(t, err, testCase.input) {
			assert.Equal(t, testCase.expected, serverConfigs)
		}
	}
}

// scconfigs allows for quickly generating server configs,
// for testing purposes
func scconfigs(argv ...interface{}) (serverConfigs []config.StorageServerConfig) {
	argn := len(argv)
	for i := 0; i < argn; i += 2 {
		serverConfigs = append(serverConfigs, config.StorageServerConfig{
			Address:  argv[i].(string),
			Database: argv[i+1].(int),
		})
	}
	return
}
