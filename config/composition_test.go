package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNBDStorageConfigValidate(t *testing.T) {
	assert := assert.New(t)

	for _, validConfig := range validNBDStorageConfigs {
		err := validConfig.Validate()
		assert.NoErrorf(err, "input: %v", validConfig)
	}

	for _, invalidConfig := range invalidNBDStorageConfigs {
		err := invalidConfig.Validate()
		if assert.Errorf(err, "input: %v", invalidConfig) {
			t.Logf("NBDStorageConfigValidate error: %v", err)
		}
	}
}

func TestTlogStorageConfigValidate(t *testing.T) {
	assert := assert.New(t)

	for _, validConfig := range validTlogStorageConfigs {
		err := validConfig.Validate()
		assert.NoErrorf(err, "input: %v", validConfig)
	}

	for _, invalidConfig := range invalidTlogStorageConfigs {
		err := invalidConfig.Validate()
		if assert.Errorf(err, "input: %v", invalidConfig) {
			t.Logf("TlogStorageConfigValidate error: %v", err)
		}
	}
}
