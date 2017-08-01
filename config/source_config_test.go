package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNilSourceConfig(t *testing.T) {
	assert := assert.New(t)

	var cfg SourceConfig

	if assert.NoError(cfg.Validate()) {
		assert.Equal(defaultFileResource, cfg.String())
	}

	cfg.Resource = ""
	if assert.NoError(cfg.Validate()) {
		assert.Equal(defaultFileResource, cfg.String())
	}
}

func TestNilSourceType(t *testing.T) {
	var crt SourceType
	assert.Equal(t, FileSourceType, crt)
}

func TestSourceTypeInequality(t *testing.T) {
	assert.NotEqual(t, ETCDSourceType, FileSourceType)
}

func TestSourceTypeToString(t *testing.T) {
	assert := assert.New(t)

	// valid config resource types
	assert.Equal(etcdSourceTypeString, ETCDSourceType.String())
	assert.Equal(fileSourceTypeString, FileSourceType.String())

	// anything else defaults to etcd as well
	assert.Equal(etcdSourceTypeString, SourceType(42).String())
}

func TestSourceTypeFromString(t *testing.T) {
	assert := assert.New(t)

	var err error
	var crt SourceType

	// etcd config resource type
	err = crt.Set(etcdSourceTypeString)
	if assert.NoError(err) {
		assert.Equal(ETCDSourceType, crt)
	}

	// file config resource type
	err = crt.Set(fileSourceTypeString)
	if assert.NoError(err) {
		assert.Equal(FileSourceType, crt)
	}

	// any other string will result in an error
	err = crt.Set("foo")
	if assert.Error(err) {
		// and the variable will remain unchanged
		assert.Equal(FileSourceType, crt)
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

func TestSourceConfigReflectivity(t *testing.T) {
	assert := assert.New(t)

	// add all valid strings together and remove spaces,
	// as spaces would mess wth our reflectivity
	validResourceStrings := append(validETCDStrings, validConfigPaths...)
	for i := range validResourceStrings {
		validResourceStrings[i] = strings.Replace(validResourceStrings[i], " ", "", -1)
	}

	// strA ==     strB       ==            StrC
	// x    == String(Set(x)) == String(Set(String(Set(x))))
	for _, strA := range validResourceStrings {
		cfg, err := NewSourceConfig(strA)
		if !assert.NoError(err, strA) {
			continue
		}

		strB := cfg.String()
		if !assert.Equal(strA, strB) {
			continue
		}

		err = cfg.Set(strB)
		if assert.NoError(err, strA) {
			continue
		}

		strC := cfg.String()
		assert.Equal(strB, strC)
	}

	for _, validETCDString := range validETCDStrings {
		cfg, err := NewSourceConfig(validETCDString)
		if assert.NoError(err, validETCDString) && assert.NotNil(cfg, validETCDString) {
			assert.Equal(ETCDSourceType, cfg.SourceType, validETCDString)
			endpoints, ok := cfg.Resource.([]string)
			if assert.True(ok, validETCDString) {
				assert.True(len(endpoints) > 0, validETCDString)
			}
		}
	}

	for _, validPath := range validConfigPaths {
		cfg, err := NewSourceConfig(validPath)
		if assert.NoError(err, validPath) && assert.NotNil(cfg, validPath) {
			assert.Equal(FileSourceType, cfg.SourceType, validPath)
			path, ok := cfg.Resource.(string)
			if assert.True(ok, validPath) {
				assert.Equal(validPath, path)
			}
		}
	}
}
