package log

import (
	"errors"

	"github.com/zero-os/0-log"
)

var (
	// ErrNilVdiskID represents an error where an empty vdiskid was provided
	ErrNilVdiskID = errors.New("empty statistics ID")
	// ErrInvalidStatisticsKey represents an error where an invalid Statisticskey was provided
	ErrInvalidStatisticsKey = errors.New("invalid statistics key")
)

// BroadcastStatistics broadcasts statistics data for 0-Core statistics monitor
// using 0-Log package
func BroadcastStatistics(vdiskID string, key StatisticsKey, value float64, op AggregationType, tags MetricTags) error {
	k, err := createKey(vdiskID, key)
	if err != nil {
		return nil
	}

	msg := zerolog.MsgStatistics{
		Value:     value,
		Key:       k,
		Operation: zerolog.AggregationType(op),
		Tags:      zerolog.MetricTags(tags),
	}
	return zerolog.Log(zerolog.LevelStatistics, msg)
}

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
type StatisticsKey string

// StatisticsKey options
const (
	StatisticsKeyIOPSRead       = StatisticsKey("vdisk.iops.read")
	StatisticsKeyIOPSWrite      = StatisticsKey("vdisk.iops.write")
	StatisticsKeyTroughputRead  = StatisticsKey("vdisk.throughput.read")
	StatisticsKeyTroughputWrite = StatisticsKey("vdisk.throughput.write")
)

// Validate validates a StatisticsKey
func (sk StatisticsKey) Validate() error {
	switch sk {
	case StatisticsKeyIOPSRead, StatisticsKeyIOPSWrite, StatisticsKeyTroughputRead, StatisticsKeyTroughputWrite:
		return nil
	default:
		return ErrInvalidStatisticsKey
	}
}

func (sk StatisticsKey) String() string {
	return string(sk)
}

// zerolog wrappers

// AggregationType represents an statistics aggregation type
type AggregationType zerolog.AggregationType

const (
	// AggregationAverages represents an averaging aggregation type
	AggregationAverages = AggregationType(zerolog.AggregationAverages)
	// AggregationDifferentiates represents a differentiating aggregation type
	AggregationDifferentiates = AggregationType(zerolog.AggregationDifferentiates)
)

// MetricTags represents statistics metric tags
type MetricTags map[string]interface{}
