// +build !go1.9

package log

import (
	"github.com/zero-os/0-log"
)

// BroadcastStatistics broadcasts statistics data for 0-Core statistics monitor
// using the 0-Log package
func BroadcastStatistics(key string, value float64, op AggregationType, tags MetricTags) error {
	k, err := createKey(vdiskID, key)
	if err != nil {
		return nil
	}

	msg := zerolog.MsgStatistics{
		Key:   key,
		Value: value,
		// aggregation operation
		Operation: zerolog.AggregationType(op),
		// metric tags for the statistic
		Tags: zerolog.MetricTags(tags),
	}

	return zerolog.Log(zerolog.LevelStatistics, msg)
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
