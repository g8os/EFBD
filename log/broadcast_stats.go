package log

import (
	"errors"
)

var (
	// ErrNilVdiskID represents an error where an empty vdiskid was provided
	ErrNilVdiskID = errors.New("unexpected nil vdiskID")
	// ErrInvalidStatisticsKey represents an error where an invalid Statisticskey was provided
	ErrInvalidStatisticsKey = errors.New("invalid statistics key")
)

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
	StatisticsKeyIOPSRead = StatisticsKey(iota)
	StatisticsKeyIOPSWrite
	StatisticsKeyTroughputRead
	StatisticsKeyTroughputWrite
	StatisticsKeyEnumLength // should always be the final enum value
)

// StatisticsKey string representations
const (
	statisticsKeyIOPSReadStr       = "vdisk.iops.read"
	statisticsKeyIOPSWriteStr      = "vdisk.iops.write"
	statisticsKeyTroughputReadStr  = "vdisk.throughput.read"
	statisticsKeyTroughputWriteStr = "vdisk.throughput.write"
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
		return ""
	}
}

// Validate validates the StatisticsKey
func (sk StatisticsKey) Validate() error {
	if sk >= StatisticsKeyEnumLength {
		return ErrInvalidStatisticsKey
	}

	return nil
}
