package log

import (
	"errors"

	"github.com/zero-os/0-log"
)

var (
	// ErrNilVdiskID represents an error where an empty vdiskid was provided
	ErrNilVdiskID = errors.New("unexpected nil vdiskID")
	// ErrInvalidStatisticsKey represents an error where an invalid Statisticskey was provided
	ErrInvalidStatisticsKey = errors.New("invalid statistics key")
)

// BroadcastStatistics broadcasts statistics data for 0-Core statistics monitor
// using the 0-Log package
func BroadcastStatistics(vdiskID string, key StatisticsKey, value float64, op AggregationType, tags MetricTags) error {
	k, err := createKey(vdiskID, key)
	if err != nil {
		return nil
	}

	msg := zerolog.MsgStatistics{
		Key:   k,
		Value: value,
		// aggregation operation
		Operation: zerolog.AggregationType(op),
		// metric tags for the statistic
		Tags: zerolog.MetricTags(tags),
	}

	return zerolog.Log(zerolog.LevelStatistics, msg)
}

// createKey turns vdiskID and StatisticsKey into a key for the zerolog.MsgStatistics
// following monitoring specs: https://github.com/zero-os/0-core/tree/master/docs/monitoring#monitoring-metrics
func createKey(vdiskID string, statkey StatisticsKey) (string, error) {
	if vdiskID == "" {
		return "", ErrNilVdiskID
	}
	err := statkey.Validate()
	if err != nil {
		return "", err
	}

	return statkey.String() + "@virt." + vdiskID, nil
}

// StatisticsKey represents a statistics key
type StatisticsKey uint8

// StatisticsKey options
const (
	StatisticsKeyIOPSRead = iota
	StatisticsKeyIOPSWrite
	StatisticsKeyTroughputRead
	StatisticsKeyTroughputWrite
)

// StatisticsKey string representations
const (
	statisticsKeyIOPSReadStr       = "vdisk.iops.read"
	statisticsKeyIOPSWriteStr      = "vdisk.iops.write"
	statisticsKeyTroughputReadStr  = "vdisk.throughput.read"
	statisticsKeyTroughputWriteStr = "vdisk.throughput.write"
	statisticsKeyNilStr            = ""
)

// String returns the string representation of the StatisticsKey
func (sk StatisticsKey) String() string {
	switch sk {
	case StatisticsKeyIOPSRead:
		return statisticsKeyIOPSReadStr
	case StatisticsKeyIOPSWrite:
		return statisticsKeyIOPSWriteStr
	case StatisticsKeyTroughputRead:
		return statisticsKeyTroughputReadStr
	case StatisticsKeyTroughputWrite:
		return statisticsKeyTroughputWriteStr
	default:
		return statisticsKeyNilStr
	}
}

// Validate validates the StatisticsKey
func (sk StatisticsKey) Validate() error {
	switch sk {
	case StatisticsKeyIOPSRead, StatisticsKeyIOPSWrite, StatisticsKeyTroughputRead, StatisticsKeyTroughputWrite:
		return nil
	default:
		return ErrInvalidStatisticsKey
	}
}

// zerolog wrappers

// AggregationType represents an statistics aggregation type
// wraps zerolog.AggregationType
type AggregationType zerolog.AggregationType

const (
	// AggregationAverages represents an averaging aggregation type
	// wraps zerolog.AggregationAverages
	AggregationAverages = AggregationType(zerolog.AggregationAverages)
	// AggregationDifferentiates represents a differentiating aggregation type
	// wraps zerolog.AggregationDifferentiates
	AggregationDifferentiates = AggregationType(zerolog.AggregationDifferentiates)
)

// MetricTags represents statistics metric tags
// wraps zerolog.MetricTags
type MetricTags zerolog.MetricTags
