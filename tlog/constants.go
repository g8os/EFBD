package tlog

import "github.com/zero-os/0-Disk"

var (
	// FirstAggregateHash is used as the prevHash value
	// for the first aggregation
	FirstAggregateHash = blockstor.NilHash
)

const (
	// LastHashPrefix is the last hash ardb key prefix
	LastHashPrefix = "last_hash_"
)

var (
	// MinSupportedVersion is the minimum supported version
	// that the tlog client and server of this version supports
	MinSupportedVersion = blockstor.NewVersion(1, 1, 0, blockstor.VersionStageAlpha)
)
