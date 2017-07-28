package zerodisk

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
