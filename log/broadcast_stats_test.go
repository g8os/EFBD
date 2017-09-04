package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatsLog(t *testing.T) {
	assert := assert.New(t)

	// test valid cases
	//without tags
	err := BroadcastStatistics("vdisk1", StatisticsKeyIOPSWrite, 1.234, AggregationAverages, nil)
	assert.NoError(err)
	//with tags
	tags := MetricTags{
		"foo":   "world",
		"hello": "bar",
	}
	err = BroadcastStatistics("vdisk1", StatisticsKeyTroughputRead, 2.345, AggregationDifferentiates, tags)
	assert.NoError(err)

	// invalid Aggregation type
	err = BroadcastStatistics("vdisk1", StatisticsKeyIOPSWrite, 3.456, AggregationType("an invalid aggregation type"), nil)
	assert.Error(err)
}

func TestCreateKey(t *testing.T) {
	assert := assert.New(t)

	validCases := []struct {
		vdiskID  string
		key      StatisticsKey
		expected string
	}{
		{
			vdiskID:  "vdisk1",
			key:      StatisticsKeyIOPSRead,
			expected: "vdisk.iops.read@virt.vdisk1",
		},
		{
			vdiskID:  "vdisk2",
			key:      StatisticsKeyIOPSWrite,
			expected: "vdisk.iops.write@virt.vdisk2",
		},
		{
			vdiskID:  "vdisk1",
			key:      StatisticsKeyTroughputRead,
			expected: "vdisk.throughput.read@virt.vdisk1",
		},
		{
			vdiskID:  "vdisk3",
			key:      StatisticsKeyTroughputWrite,
			expected: "vdisk.throughput.write@virt.vdisk3",
		},
	}

	for _, c := range validCases {
		k, _ := createKey(c.vdiskID, c.key)
		assert.Equal(c.expected, k)
	}

	invalidCases := []struct {
		vdiskID  string
		key      StatisticsKey
		expected error
	}{
		{
			vdiskID:  "",
			key:      StatisticsKeyIOPSRead,
			expected: ErrNilVdiskID,
		},
		{
			vdiskID:  "vdisk1",
			key:      StatisticsKey(255),
			expected: ErrInvalidStatisticsKey,
		},
	}

	for _, c := range invalidCases {
		_, err := createKey(c.vdiskID, c.key)
		assert.Equal(c.expected, err)
	}
}
