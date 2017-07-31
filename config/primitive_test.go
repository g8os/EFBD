package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	yaml "gopkg.in/yaml.v2"
)

var invalidVdiskTypes = []string{
	"foo",
	"123",
}

var validVDiskTypeCases = []struct {
	String string
	Type   VdiskType
}{
	{vdiskTypeBootStr, VdiskTypeBoot},
	{vdiskTypeCacheStr, VdiskTypeCache},
	{vdiskTypeDBStr, VdiskTypeDB},
	{vdiskTypeTmpStr, VdiskTypeTmp},
}

func TestVdiskTypeValidate(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validVDiskTypeCases {
		assert.NoError(validCase.Type.Validate())
	}

	assert.Error(VdiskType(0).Validate())
	assert.Error(VdiskType(255).Validate())
}

func TestValidVdiskTypeDeserialization(t *testing.T) {
	assert := assert.New(t)

	var vdiskType VdiskType
	for _, validCase := range validVDiskTypeCases {
		err := yaml.Unmarshal([]byte(validCase.String), &vdiskType)
		if !assert.NoError(err, "unexpected invalid type: %q", validCase.String) {
			continue
		}

		assert.Equal(validCase.Type, vdiskType)
	}
}

func TestValidVdiskTypeSerialization(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validVDiskTypeCases {
		bytes, err := yaml.Marshal(validCase.Type)
		if !assert.NoError(err) {
			continue
		}

		str := strings.Trim(string(bytes), "\n")
		assert.Equal(validCase.String, str)
	}
}

func TestInvalidVdiskTypeDeserialization(t *testing.T) {
	assert := assert.New(t)

	var vdiskType VdiskType
	for _, invalidType := range invalidVdiskTypes {
		err := yaml.Unmarshal([]byte(invalidType), &vdiskType)
		assert.Errorf(err, "unexpected valid type: %q", invalidType)
	}
}

func TestVdiskProperties(t *testing.T) {
	assert := assert.New(t)

	// validate storage type property
	assert.Equal(StorageDeduped, VdiskTypeBoot.StorageType())
	assert.Equal(StorageNonDeduped, VdiskTypeDB.StorageType())
	assert.Equal(StorageNonDeduped, VdiskTypeCache.StorageType())
	assert.Equal(StorageNonDeduped, VdiskTypeTmp.StorageType())

	// validate tlog support
	assert.True(VdiskTypeBoot.TlogSupport())
	assert.True(VdiskTypeDB.TlogSupport())
	assert.False(VdiskTypeCache.TlogSupport())
	assert.False(VdiskTypeTmp.TlogSupport())

	// validate template support
	assert.True(VdiskTypeBoot.TemplateSupport())
	assert.True(VdiskTypeDB.TemplateSupport())
	assert.False(VdiskTypeCache.TemplateSupport())
	assert.False(VdiskTypeTmp.TemplateSupport())
}
