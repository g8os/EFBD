package configV2

import (
	"strings"
	"testing"

	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
)

func TestSubConfigCloning(t *testing.T) {
	// setup original tlog
	tlog0, err := newTlogConfig([]byte(validTlogStr))
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog0) {
		return
	}

	// clone
	tlog1 := tlog0.Clone()

	// change field in original
	tlog0.SlaveSync = false
	newDataAddress := "192.168.1.1:1234"
	oldDataAddress := tlog0.TlogStorageCluster.DataStorage[0].Address
	tlog0.TlogStorageCluster.DataStorage[0].Address = newDataAddress

	// check if change did not appear in clone
	assert.False(t, tlog0.SlaveSync)
	assert.True(t, tlog1.SlaveSync)
	assert.Equal(t, tlog0.TlogStorageCluster.DataStorage[0].Address, newDataAddress)
	if !assert.Equal(t, tlog1.TlogStorageCluster.DataStorage[0].Address, oldDataAddress) {
		return
	}

	// setup original nbd
	vdiskID := "test"
	vdiskType := VdiskTypeBoot
	nbd0, err := newNBDConfig([]byte(validNBDStr), vdiskID, vdiskType)
	if !assert.NoError(t, err) || !assert.NotNil(t, nbd0) {
		return
	}

	// clone
	nbd1 := nbd0.Clone()

	// change fields
	oldTemplateID := nbd0.TemplateVdiskID
	newTemplateID := "anotherTemplate"
	nbd0.TemplateVdiskID = newTemplateID
	newDataDB := 123
	oldDataDB := nbd0.TemplateStorageCluster.DataStorage[0].Database
	nbd0.TemplateStorageCluster.DataStorage[0].Database = newDataDB

	// check if changes did not appear in clone
	assert.Equal(t, nbd0.TemplateVdiskID, newTemplateID)
	assert.Equal(t, nbd1.TemplateVdiskID, oldTemplateID)
	assert.Equal(t, nbd0.TemplateStorageCluster.DataStorage[0].Database, newDataDB)
	assert.Equal(t, nbd1.TemplateStorageCluster.DataStorage[0].Database, oldDataDB)
}

func TestInvalidConfigs(t *testing.T) {
	for _, input := range invalidNBDServerConfigs {
		_, err := fromYAMLBytes([]byte(input))
		if !assert.Error(t, err, input) {
			return
		}
	}
}

func TestSerializing(t *testing.T) {
	// create base config from a valid base config string
	b, err := newBaseConfig([]byte(validBaseStr))
	if !assert.NoError(t, err) {
		return
	}
	// serialise and deserialise
	b2Str, err := b.ToBytes()
	if !assert.NoError(t, err) {
		return
	}
	b2, err := newBaseConfig(b2Str)
	if !assert.NoError(t, err) {
		return
	}
	// check if valid base config and values are persisted
	if !assert.Equal(t, b.BlockSize, b2.BlockSize) {
		return
	}
	if !assert.Equal(t, b.ReadOnly, b2.ReadOnly) {
		return
	}
	if !assert.Equal(t, b.Size, b2.Size) {
		return
	}
	if !assert.Equal(t, b.Type, b2.Type) {
		return
	}
}

func TestValidVdiskTypeSerialization(t *testing.T) {
	for _, validCase := range validVDiskTypeCases {
		bytes, err := yaml.Marshal(validCase.Type)
		if !assert.NoError(t, err) {
			continue
		}

		str := strings.Trim(string(bytes), "\n")
		assert.Equal(t, validCase.String, str)
	}
}
