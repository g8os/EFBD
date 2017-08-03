package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNBDStorageConfigValidateNonDeduped(t *testing.T) {
	assert := assert.New(t)

	for _, validConfig := range validNBDStorageConfigs {
		err := validConfig.Validate(StorageNonDeduped)
		assert.NoErrorf(err, "input: %v", validConfig)
	}

	for _, invalidConfig := range invalidNBDStorageConfigs {
		err := invalidConfig.Validate(StorageNonDeduped)
		if assert.Errorf(err, "input: %v", invalidConfig) {
			t.Logf("NBDStorageConfigValidate error: %v", err)
		}
	}
}

func TestNBDStorageConfigValidateDeduped(t *testing.T) {
	assert := assert.New(t)

	for _, validConfig := range validNBDStorageConfigsDeduped {
		err := validConfig.Validate(StorageDeduped)
		assert.NoErrorf(err, "input: %v", validConfig)
	}

	for _, invalidConfig := range invalidNBDStorageConfigsDeduped {
		err := invalidConfig.Validate(StorageDeduped)
		if assert.Errorf(err, "input: %v", invalidConfig) {
			t.Logf("NBDStorageConfigValidate error: %v", err)
		}
	}
}

func TestTlogStorageConfigValidateNonDeduped(t *testing.T) {
	assert := assert.New(t)

	for _, validConfig := range validTlogStorageConfigs {
		err := validConfig.Validate(StorageNonDeduped)
		assert.NoErrorf(err, "input: %v", validConfig)
	}

	for _, invalidConfig := range invalidTlogStorageConfigs {
		err := invalidConfig.Validate(StorageNonDeduped)
		if assert.Errorf(err, "input: %v", invalidConfig) {
			t.Logf("TlogStorageConfigValidate error: %v", err)
		}
	}
}

func TestTlogStorageConfigValidateDeduped(t *testing.T) {
	assert := assert.New(t)

	for _, validConfig := range validTlogStorageConfigsDeduped {
		err := validConfig.Validate(StorageDeduped)
		assert.NoErrorf(err, "input: %v", validConfig)
	}

	for _, invalidConfig := range invalidTlogStorageConfigsDeduped {
		err := invalidConfig.Validate(StorageDeduped)
		if assert.Errorf(err, "input: %v", invalidConfig) {
			t.Logf("TlogStorageConfigValidate error: %v", err)
		}
	}
}
