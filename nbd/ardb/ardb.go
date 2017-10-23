package ardb

// shared constants
const (
	// DefaultLBACacheLimit defines the default cache limit
	DefaultLBACacheLimit = 20 * MebibyteAsBytes // 20 MiB
	// constants used to convert between MiB/GiB and bytes
	GibibyteAsBytes int64 = 1024 * 1024 * 1024
	MebibyteAsBytes int64 = 1024 * 1024
)
