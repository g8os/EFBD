package config

import (
	"strings"
	"testing"

	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
)

func TestSerializing(t *testing.T) {
	// create base config from a valid base config string
	b, err := NewBaseConfig([]byte(validBaseStr))
	if !assert.NoError(t, err) || !assert.NotNil(t, b) {
		return
	}
	// validate
	err = b.Validate()
	if !assert.NoError(t, err) {
		return
	}
	// serialise and deserialise
	b2Str, err := b.ToBytes()
	if !assert.NoError(t, err) {
		return
	}
	b2, err := NewBaseConfig(b2Str)
	if !assert.NoError(t, err) || !assert.NotNil(t, b2) {
		return
	}
	// check if values are persisted
	assert.Equal(t, b.BlockSize, b2.BlockSize)
	assert.Equal(t, b.ReadOnly, b2.ReadOnly)
	assert.Equal(t, b.Size, b2.Size)
	assert.Equal(t, b.Type, b2.Type)

	// create nbd config from valid nbd config string
	n, err := NewNBDConfig([]byte(validNBDStr), b.Type)
	if !assert.NoError(t, err) || !assert.NotNil(t, n) {
		return
	}
	// validate
	err = n.Validate(VdiskTypeBoot)
	if !assert.NoError(t, err) {
		return
	}

	// serialise and deserialise
	n2Str, err := n.ToBytes()
	if !assert.NoError(t, err) {
		return
	}
	n2, err := NewNBDConfig(n2Str, b.Type)
	if !assert.NoError(t, err) || !assert.NotNil(t, b2) {
		return
	}
	// check if values are persisted
	assert.Equal(t, n.TemplateVdiskID, n2.TemplateVdiskID)
	assert.Equal(t, n.StorageCluster.MetadataStorage.Address, n2.StorageCluster.MetadataStorage.Address)
	assert.Equal(t, n.StorageCluster.DataStorage[0].Address, n2.StorageCluster.DataStorage[0].Address)
	assert.Equal(t, n.TemplateStorageCluster.DataStorage[0].Address, n2.TemplateStorageCluster.DataStorage[0].Address)

	// create tlog config from valid tlog config string
	tlog, err := NewTlogConfig([]byte(validTlogStr))
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog) {
		return
	}
	// validate
	err = tlog.Validate()
	if !assert.NoError(t, err) {
		return
	}

	// serialise and deserialise
	tlog2Str, err := tlog.ToBytes()
	tlog2, err := NewTlogConfig(tlog2Str)
	if !assert.NoError(t, err) || !assert.NotNil(t, tlog2) {
		return
	}
	// check if values are persisted
	assert.Equal(t, tlog.TlogStorageCluster.MetadataStorage.Address, tlog2.TlogStorageCluster.MetadataStorage.Address)
	assert.Equal(t, tlog.TlogStorageCluster.DataStorage[0].Address, tlog2.TlogStorageCluster.DataStorage[0].Address)
	assert.Equal(t, tlog.SlaveSync, tlog2.SlaveSync)

	// create s config from valid s config string
	s, err := NewSlaveConfig([]byte(validSlaveStr))
	if !assert.NoError(t, err) || !assert.NotNil(t, s) {
		return
	}
	// validate
	err = s.Validate()
	if !assert.NoError(t, err) {
		return
	}

	// serialise and deserialise
	s2Str, err := s.ToBytes()
	s2, err := NewSlaveConfig(s2Str)
	if !assert.NoError(t, err) || !assert.NotNil(t, s2) {
		return
	}
	// check if values are persisted
	assert.Equal(t, s.SlaveStorageCluster.MetadataStorage.Address, s2.SlaveStorageCluster.MetadataStorage.Address)
	assert.Equal(t, s.SlaveStorageCluster.DataStorage[0].Address, s2.SlaveStorageCluster.DataStorage[0].Address)
}

func TestInvalidConfigs(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	log.Debug("Testing invalid configs, logging the errors")
	for i, input := range invalidNBDServerConfigs {
		// from YAMLbytes returns an error if config not valid
		// check log if errors match intended fails
		_, err := readVdiskConfigBytes("testVdisk", []byte(input))
		log.Debugf("%v: %v", i+1, err)
		if !assert.Error(t, err, input) {
			return
		}
	}
	log.Debug("Done testing invalid configs")
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
