package configV2

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidConfig(t *testing.T) {
	cfg, err := fromYAMLBytes([]byte(validConfigStr))
	if !assert.NoError(t, err) {
		fmt.Println(cfg)
		return
	}
}

func TestInvalidNDBConfigs(t *testing.T) {
	for _, input := range invalidNBDServerConfigs {
		_, err := fromYAMLBytes([]byte(input))

		if !assert.Error(t, err, input) {

			return
		}
	}
}
func TestSerializing(t *testing.T) {

	// get config from a valid full config string
	cfg, err := fromYAMLBytes([]byte(validConfigStr))
	if !assert.NoError(t, err) {
		return
	}

	err = cfg.validate()
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
