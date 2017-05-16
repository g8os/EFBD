package tlog

import "github.com/g8os/blockstor"

var (
	// used as the prevHash value for the first aggregation
	FirstAggregateHash = blockstor.NilHash
)

const (
	// last hash ardb key prefix
	LastHashPrefix = "last_hash_"
)
