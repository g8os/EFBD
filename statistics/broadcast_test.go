package statistics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatsLog(t *testing.T) {
	assert := assert.New(t)

	// test valid cases
	//without tags
	err := Broadcast("vdisk1", KeyIOPSWrite, 1.234, AggregationAverages, nil)
	assert.NoError(err)
	//with tags
	tags := MetricTags{
		"foo":   "world",
		"hello": "bar",
	}
	err = Broadcast("vdisk1", KeyTroughputRead, 2.345, AggregationDifferentiates, tags)
	assert.NoError(err)

	// invalid Aggregation type
	err = Broadcast("vdisk1", KeyIOPSWrite, 3.456, AggregationType("an invalid aggregation type"), nil)
	assert.Error(err)
}

func TestCreateKey(t *testing.T) {
	assert := assert.New(t)

	validCases := []struct {
		vdiskID  string
		key      Key
		expected string
	}{
		{
			vdiskID:  "vdisk1",
			key:      KeyIOPSRead,
			expected: "vdisk.iops.read@virt.vdisk1",
		},
		{
			vdiskID:  "vdisk2",
			key:      KeyIOPSWrite,
			expected: "vdisk.iops.write@virt.vdisk2",
		},
		{
			vdiskID:  "vdisk1",
			key:      KeyTroughputRead,
			expected: "vdisk.throughput.read@virt.vdisk1",
		},
		{
			vdiskID:  "vdisk3",
			key:      KeyTroughputWrite,
			expected: "vdisk.throughput.write@virt.vdisk3",
		},
	}

	for _, c := range validCases {
		k, _ := createKey(c.vdiskID, c.key)
		assert.Equal(c.expected, k)
	}

	invalidCases := []struct {
		vdiskID  string
		key      Key
		expected error
	}{
		{
			vdiskID:  "",
			key:      KeyIOPSRead,
			expected: ErrNilVdiskID,
		},
		{
			vdiskID:  "vdisk1",
			key:      Key(255),
			expected: ErrInvalidKey,
		},
	}

	for _, c := range invalidCases {
		_, err := createKey(c.vdiskID, c.key)
		assert.Equal(c.expected, err)
	}
}
