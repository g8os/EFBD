package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadNBDVdisksConfig(t *testing.T) {
	assert := assert.New(t)

	_, err := ReadNBDVdisksConfig(nil, "foo")
	assert.Error(err, "should trigger error due to nil-source")

	// create stub source, with no config, which will trigger errors
	source := newStubSource(nil)

	_, err = ReadNBDVdisksConfig(source, "foo")
	assert.Error(err, "should trigger error due to nil config")

	source.SetVdiskConfig("a", new(fileFormatVdiskConfig))
	_, err = ReadNBDVdisksConfig(source, "foo")
	assert.Error(err, "should trigger error due to invalid config")

	source.SetVdiskConfig("a", newValidStubVdiskConfig(VdiskTypeBoot, "foo"))
	vdisksCfg, err := ReadNBDVdisksConfig(source, "foo")
	if assert.NoError(err, "should return us 'a'") && assert.Len(vdisksCfg.Vdisks, 1) {
		assert.Equal("a", vdisksCfg.Vdisks[0])
	}

	source.SetVdiskConfig("b", newValidStubVdiskConfig(VdiskTypeBoot, "foo"))
	vdisksCfg, err = ReadNBDVdisksConfig(source, "foo")
	if assert.NoError(err, "should return us 'a, b'") && assert.Len(vdisksCfg.Vdisks, 2) {
		assert.Subset([]string{"a", "b"}, vdisksCfg.Vdisks)
	}
}
