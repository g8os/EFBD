package configV2

import (
	"strings"
	"testing"

	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
)

func TestValidYAMLConfig(t *testing.T) {
	_, err := fromYAMLBytes([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}
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
	// get config from a valid full config string
	_, err := fromYAMLBytes([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) {
		return
	}

	if !assert.NoError(t, err) {
		return
	}

	// create base config from a valid base config string
	b := new(BaseConfig)
	err = b.deserialize([]byte(validBaseStr))
	if !assert.NoError(t, err) {
		return
	}
	// serialise and deserialise
	b2 := new(BaseConfig)
	b2Str, err := b.serialize()
	if !assert.NoError(t, err) {
		return
	}
	err = b2.deserialize(b2Str)
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

func TestYAMLSource(t *testing.T) {
	cfg, err := fromYAMLBytes([]byte(validYAMLSourceStr))
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}
	defer func() {
		err = cfg.Close()
		assert.NoError(t, err)
	}()

	base := BaseConfig{
		BlockSize: 1234,
		ReadOnly:  true,
		Size:      2,
		Type:      VdiskTypeBoot,
	}

	ndb := cfg.ndb
	ndb.TemplateVdiskID = "test"

	tlog := cfg.tlog
	tlog.TlogStorageCluster.DataStorage[0].Address = "A new address"

	slave, err := newSlaveConfig([]byte(validSlaveStr))
	if !assert.NoError(t, err) {
		return
	}

	// test if new base is set
	err = cfg.SetBase(base)
	if !assert.Equal(t, base.BlockSize, cfg.base.BlockSize) {
		return
	}
	if !assert.Equal(t, base.ReadOnly, cfg.base.ReadOnly) {
		return
	}
	if !assert.Equal(t, base.Size, cfg.base.Size) {
		return
	}
	if !assert.Equal(t, base.Type, cfg.base.Type) {
		return
	}

	// test if new ndb is set
	cfg.SetNDB(ndb)
	if !assert.Equal(t, ndb.TemplateVdiskID, cfg.ndb.TemplateVdiskID) {
		return
	}

	// test if new tlog is set
	cfg.SetTlog(tlog)
	if !assert.Equal(t, tlog.TlogStorageCluster.DataStorage[0].Address, cfg.tlog.TlogStorageCluster.DataStorage[0].Address) {
		return
	}

	// test if new slave is set
	cfg.SetSlave(*slave)
	if !assert.Equal(t, slave.SlaveStorageCluster.DataStorage[0].Address, cfg.slave.SlaveStorageCluster.DataStorage[0].Address) {
		return
	}
}
