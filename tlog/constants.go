package tlog

import "github.com/zero-os/0-Disk"

const (
	// FirstSequence is the first sequence number used by tlog
	FirstSequence = uint64(1)
)

var (
	// MinSupportedVersion is the minimum supported version
	// that the tlog client and server of this version supports
	MinSupportedVersion = zerodisk.NewVersion(1, 1, 0, zerodisk.VersionStageAlpha)
)
