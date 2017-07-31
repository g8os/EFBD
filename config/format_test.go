package config

import (
	"testing"

	valid "github.com/asaskevich/govalidator"
	"github.com/stretchr/testify/assert"
	yaml "gopkg.in/yaml.v2"
)

// tests NBDVdisksConfig bytes constructor and Validate function.
func TestNewNBDVdisksConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validNBDVdisksConfigYAML {
		cfg, err := NewNBDVdisksConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			if assert.NotNil(cfg, validCase) {
				assert.NotEmpty(cfg.Vdisks, validCase)
			}
		}
	}

	for _, invalidCase := range invalidNBDVdisksConfigYAML {
		cfg, err := NewNBDVdisksConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewNBDVdisksConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

// tests VdiskStaticConfig bytes constructor and Validate function.
func TestNewVdiskStaticConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validVdiskStaticConfigYAML {
		cfg, err := NewVdiskStaticConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			assert.NotNil(cfg, validCase)
		}
	}

	for _, invalidCase := range invalidVdiskStaticConfigYAML {
		cfg, err := NewVdiskStaticConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewVdiskStaticConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

// tests VdiskNBDConfig bytes constructor and Validate function.
func TestNewVdiskNBDConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validVdiskNBDConfigYAML {
		cfg, err := NewVdiskNBDConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			assert.NotNil(cfg, validCase)
		}
	}

	for _, invalidCase := range invalidVdiskNBDConfigYAML {
		cfg, err := NewVdiskNBDConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewVdiskNBDConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

// tests VdiskTlogConfig bytes constructor and Validate function.
func TestNewVdiskTlogConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validVdiskTlogConfigYAML {
		cfg, err := NewVdiskTlogConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			assert.NotNil(cfg, validCase)
		}
	}

	for _, invalidCase := range invalidVdiskTlogConfigYAML {
		cfg, err := NewVdiskTlogConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewVdiskTlogConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

// tests StorageClusterConfig bytes constructor and Validate function.
func TestNewStorageClusterConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validStorageClusterConfigYAML {
		cfg, err := NewStorageClusterConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			assert.NotNil(cfg, validCase)
		}
	}

	for _, invalidCase := range invalidStorageClusterConfigYAML {
		cfg, err := NewStorageClusterConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewStorageClusterConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

// tests TlogServerConfig manual unmarshalling
func TestTlogServerConfigStrictYamlUnmarshal(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validTlogServerConfigYAML {
		var cfg TlogServerConfig
		err := yaml.UnmarshalStrict([]byte(validCase), &cfg)
		if assert.NoErrorf(err, "'%v'", validCase) {
			_, err = valid.ValidateStruct(&cfg)
			assert.NoErrorf(err, "'%v'", validCase)
		}
	}

	for _, invalidCase := range invalidTlogServerConfigYAML {
		var cfg TlogServerConfig
		err := yaml.UnmarshalStrict([]byte(invalidCase), &cfg)
		if err == nil {
			_, err = valid.ValidateStruct(&cfg)
			if assert.Errorf(err, "'%v' -> %v'", invalidCase, cfg) {
				t.Logf("TlogServerConfig error: %v", err)
			}
		} else {
			t.Logf("TlogServerConfig error: %v", err)
		}
	}
}

// tests StorageServerConfig manual unmarshalling
func TestStorageServerConfigStrictYamlUnmarshal(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validStorageServerConfigYAML {
		var cfg StorageServerConfig
		err := yaml.UnmarshalStrict([]byte(validCase), &cfg)
		if assert.NoErrorf(err, "'%v'", validCase) {
			_, err = valid.ValidateStruct(&cfg)
			assert.NoErrorf(err, "'%v'", validCase)
		}
	}

	for _, invalidCase := range invalidStorageServerConfigYAML {
		var cfg TlogServerConfig
		err := yaml.UnmarshalStrict([]byte(invalidCase), &cfg)
		if err == nil {
			_, err = valid.ValidateStruct(&cfg)
			if assert.Errorf(err, "'%v' -> %v'", invalidCase, cfg) {
				t.Logf("StorageServerConfig error: %v", err)
			}
		} else {
			t.Logf("StorageServerConfig error: %v", err)
		}
	}
}
