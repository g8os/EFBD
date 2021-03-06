package badger

import (
	"time"

	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
)

// Options are params for creating DB object.
type Options struct {
	// 1. Mandatory flags
	// -------------------
	// Directory to store the data in. Should exist and be writable.
	Dir string
	// Directory to store the value log in. Can be the same as Dir. Should exist and be writable.
	ValueDir string

	// 2. Frequently modified flags
	// -----------------------------
	// Sync all writes to disk. Setting this to true would slow down data loading significantly.
	SyncWrites bool

	// How should LSM tree be accessed.
	TableLoadingMode options.FileLoadingMode

	// How often to run value log garbage collector. Every time it runs, there'd be a spike in LSM
	// tree activity. But, running it frequently allows reclaiming disk space from an ever-growing
	// value log.
	ValueGCRunInterval time.Duration

	// 3. Flags that user might want to review
	// ----------------------------------------
	// The following affect all levels of LSM tree.
	MaxTableSize        int64 // Each table (or file) is at most this size.
	LevelSizeMultiplier int   // Equals SizeOf(Li+1)/SizeOf(Li).
	MaxLevels           int   // Maximum number of levels of compaction.
	ValueThreshold      int   // If value size >= this threshold, only store value offsets in tree.
	NumMemtables        int   // Maximum number of tables to keep in memory, before stalling.

	// The following affect how we handle LSM tree L0.
	// Maximum number of Level 0 tables before we start compacting.
	NumLevelZeroTables int

	// If we hit this number of Level 0 tables, we will stall until L0 is compacted away.
	NumLevelZeroTablesStall int

	// Maximum total size for L1.
	LevelOneSize int64

	// Run value log garbage collection if we can reclaim at least this much space. This is a ratio.
	ValueGCThreshold float64

	// Size of single value log file.
	ValueLogFileSize int64

	// Number of compaction workers to run concurrently.
	NumCompactors int

	// 4. Flags for testing purposes
	// ------------------------------
	DoNotCompact bool // Stops LSM tree from compactions.

	maxBatchSize int64 // max batch size in bytes
}

// DefaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs.
var DefaultOptions = Options{
	DoNotCompact:        false,
	LevelOneSize:        256 << 20,
	LevelSizeMultiplier: 10,
	TableLoadingMode:    options.LoadToRAM,
	// table.MemoryMap to mmap() the tables.
	// table.Nothing to not preload the tables.
	MaxLevels:               7,
	MaxTableSize:            64 << 20,
	NumCompactors:           3,
	NumLevelZeroTables:      5,
	NumLevelZeroTablesStall: 10,
	NumMemtables:            5,
	SyncWrites:              false,
	// Nothing to read/write value log using standard File I/O
	// MemoryMap to mmap() the value log files
	ValueGCRunInterval: 10 * time.Minute,
	ValueGCThreshold:   0.5, // Set to zero to not run GC.
	ValueLogFileSize:   1 << 30,
	ValueThreshold:     20,
}

func (opt *Options) estimateSize(entry *Entry) int {
	if len(entry.Value) < opt.ValueThreshold {
		return len(entry.Key) + len(entry.Value) + y.MetaSize + y.UserMetaSize + y.CasSize
	}
	return len(entry.Key) + 16 + y.MetaSize + y.UserMetaSize + y.CasSize
}
