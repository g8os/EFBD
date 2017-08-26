// +build go1.9

package log

import (
	"github.com/zero-os/0-log"
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
		Operation: op,
		// metric tags for the statistic
		Tags: tags,
	}

	return zerolog.Log(zerolog.LevelStatistics, msg)
}

// zerolog wrappers

// AggregationType represents an statistics aggregation type
// wraps zerolog.AggregationType
type AggregationType = zerolog.AggregationType

const (
	// AggregationAverages represents an averaging aggregation type
	// wraps zerolog.AggregationAverages
	AggregationAverages = zerolog.AggregationAverages
	// AggregationDifferentiates represents a differentiating aggregation type
	// wraps zerolog.AggregationDifferentiates
	AggregationDifferentiates = zerolog.AggregationDifferentiates
)

// MetricTags represents statistics metric tags
// wraps zerolog.MetricTags
type MetricTags = zerolog.MetricTags
