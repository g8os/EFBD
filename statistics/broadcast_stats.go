package statistics

import (
	"errors"
)

var (
	// ErrNilVdiskID represents an error where an empty vdiskid was provided
	ErrNilVdiskID = errors.New("unexpected nil vdiskID")
	// ErrInvalidKey represents an error where an invalid Key was provided
	ErrInvalidKey = errors.New("invalid statistics key")
)

// createKey turns vdiskID and Key into a key for the zerolog.MsgStatistics
// following monitoring specs: https://github.com/zero-os/0-core/tree/master/docs/monitoring#monitoring-metrics
func createKey(vdiskID string, statkey Key) (string, error) {
	if vdiskID == "" {
		return "", ErrNilVdiskID
	}
	err := statkey.Validate()
	if err != nil {
		return "", err
	}

	return statkey.String() + "@virt." + vdiskID, nil
}

// Key represents a statistics key
type Key uint8

// Key options
const (
	KeyIOPSRead = Key(iota)
	KeyIOPSWrite
	KeyTroughputRead
	KeyTroughputWrite
	KeyEnumLength // should always be the final enum value
)

// Key string representations
const (
	KeyIOPSReadStr       = "vdisk.iops.read"
	KeyIOPSWriteStr      = "vdisk.iops.write"
	KeyTroughputReadStr  = "vdisk.throughput.read"
	KeyTroughputWriteStr = "vdisk.throughput.write"
)

// String returns the string representation of the Key
func (sk Key) String() string {
	switch sk {
	case KeyIOPSRead:
		return KeyIOPSReadStr
	case KeyIOPSWrite:
		return KeyIOPSWriteStr
	case KeyTroughputRead:
		return KeyTroughputReadStr
	case KeyTroughputWrite:
		return KeyTroughputWriteStr
	default:
		return ""
	}
}

// Validate validates the Key
func (sk Key) Validate() error {
	if sk >= KeyEnumLength {
		return ErrInvalidKey
	}

	return nil
}
