package backup

import (
	"errors"
	"fmt"
	"io"
	"runtime"

	"github.com/zero-os/0-Disk/config"
)

const (
	// DefaultBlockSize is the default block size,
	// used for the deduped blocks stored as a backup.
	DefaultBlockSize = 1024 * 128 // 128 KiB
)

// Config used to export/import a backup.
type Config struct {
	// Required: VdiskID to export from or import into
	VdiskID string
	// Optional: ID of the snapshot (same as VdiskID by default)
	SnapshotID string

	// Optional: Snapshot BlockSize (128KiB by default)
	// NOTE: only used by export funcs (ignored by import funcs)
	BlockSize int64

	// Required: SourceConfig to configure the storage with
	BlockStorageConfig config.SourceConfig

	// Optional: BackupStoragDriverConfig used to configure the backup storage driver.
	//  - When not given, defaults to LocalStorageDriver, using the DefaultLocalRoot as the path.
	//  - When given:
	//    - If type equals LocalStorageDriverConfig -> Create LocalStorageDriver;
	//    - If type equals FTPStorageDriverConfig -> Create FTPStorageDriver;
	//    - Else -> error
	BackupStoragDriverConfig interface{}

	// Optional: Amount of jobs (goroutines) to run simultaneously
	//           (to import/export in parallel)
	//           By default it equals the amount of CPUs available.
	JobCount int

	// Type of Compression to use for compressing/decompressing.
	// Note: this should be the same value for an import/export pair
	CompressionType CompressionType
	// CryptoKey to use for encryption/decryption.
	// Note: this should be the same value for an import/export pair
	CryptoKey CryptoKey

	// Optional: Only used for exporting at the moment.
	// When true, a new deduped map will be created in case
	// the existing deduped map couldn't be loaded.
	// This can happen due to the fact that the existing map's data is corrupt,
	// or the data was encrypted/compressed using a different
	// key/compressionType than the one given.
	Force bool
}

// validate the export/import config,
// and fill-in all the missing optional data.
func (cfg *Config) validate() error {
	if cfg.VdiskID == "" {
		return errNilVdiskID
	}
	if cfg.SnapshotID == "" {
		cfg.SnapshotID = cfg.VdiskID
	}

	// turn this into config.ValidateBlockSize(x)
	if cfg.BlockSize == 0 {
		cfg.BlockSize = DefaultBlockSize
	} else {
		if !config.ValidateBlockSize(cfg.BlockSize) {
			return fmt.Errorf("blockSize '%d' is not valid", cfg.BlockSize)
		}
	}

	err := cfg.BlockStorageConfig.Validate()
	if err != nil {
		return err
	}

	if cfg.JobCount <= 0 {
		cfg.JobCount = runtime.NumCPU()
	}

	err = cfg.CompressionType.validate()
	if err != nil {
		return err
	}

	return nil
}

// create a storage driver based on the given backup storage driver config.
func (cfg *Config) createStorageDriver() (StorageDriver, error) {
	if cfg.BackupStoragDriverConfig == nil {
		// default to DefaultLocalRoot, of nothing is given by the user.
		return LocalStorageDriver(LocalStorageDriverConfig{
			Path: DefaultLocalRoot,
		})
	}

	// if a config is given, try to interpret it
	switch sdCfg := cfg.BackupStoragDriverConfig.(type) {
	case FTPStorageDriverConfig:
		return FTPStorageDriver(sdCfg)

	case LocalStorageDriverConfig:
		return LocalStorageDriver(sdCfg)

	default:
		return nil, fmt.Errorf(
			"%[1]v (%[1]T) is not a valid BackupStoragDriverConfig", sdCfg)
	}
}

// storageConfig returned when creating a block storage,
// ready to export to/import from a backup.
type storageConfig struct {
	NBD   config.NBDStorageConfig
	Vdisk config.VdiskStaticConfig
}

// blockFetcher is a generic interface which defines the API
// to fetch a block (and its index) until we io.EOF is reached.
type blockFetcher interface {
	// FetchBlock fetches a new block (and its index) every call,
	// io.EOF is returned in case no blocks are available any longer.
	FetchBlock() (*blockIndexPair, error)
}

// blockIndexPair is the result type for the `blockFetcher` API.
type blockIndexPair struct {
	// Block which has been fetched.
	Block []byte
	// Index of the Block which has been fetched.
	Index int64
}

// sizedBlockFetcher wraps the given blockFetcher,
// in case the dst- and src- blocksize don't match up.
// This way you can be sure that you're block fetcher,
// always returns blocks that match the expected destination size.
func sizedBlockFetcher(bf blockFetcher, srcBS, dstBS int64) blockFetcher {
	if srcBS < dstBS {
		return newInflationBlockFetcher(bf, srcBS, dstBS)
	}

	if srcBS > dstBS {
		return newDeflationBlockFetcher(bf, srcBS, dstBS)
	}

	// srcBS == dstBS
	return bf
}

// newInflationBlockFetcher creates a new Inflation BlockFetcher,
// wrapping around the given block fetcher.
// See `inflationBlockFetcher` for more information.
func newInflationBlockFetcher(bf blockFetcher, srcBS, dstBS int64) *inflationBlockFetcher {
	return &inflationBlockFetcher{
		in:    bf,
		srcBS: srcBS,
		dstBS: dstBS,
		ratio: dstBS / srcBS,
	}
}

// inflationBlockFetcher allows you to fetch bigger blocks,
// from an internal blockFetcher which itself returns smaller blocks.
type inflationBlockFetcher struct {
	in           blockFetcher
	srcBS, dstBS int64
	ratio        int64

	cache struct {
		output    []byte
		offset    int64
		prevIndex int64
	}
}

// FetchBlock implements blockFetcher.FetchBlock
func (ibf *inflationBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	var err error
	var indexDelta int64
	var blockPair *blockIndexPair

	// only start a new offset/output
	// if we don't have an output left from last time
	if ibf.cache.output == nil {
		blockPair, err = ibf.in.FetchBlock()
		if err != nil {
			return nil, err
		}

		ibf.newOutputBlock(blockPair)
	}

	// try to fill up the (bigger) destination block as much as possible
	for ibf.cache.offset < ibf.dstBS {
		// we have still space for an actual block, let's fetch it
		blockPair, err = ibf.in.FetchBlock()
		if err != nil {
			if err == io.EOF {
				break // this is OK, as we'll just concider the rest of dst block as 0
			}

			// keep current output in cache, as we aren't done yet
			return nil, err
		}

		// if our delta is bigger than 1,
		// we need to first move our offset, as to respect the original block spacing.
		indexDelta = blockPair.Index - ibf.cache.prevIndex
		if ibf.cache.prevIndex >= 0 && indexDelta > 1 {
			ibf.cache.offset += (indexDelta - 1) * ibf.srcBS
			// if the offset goes now beyond the destination block size,
			// we can return the output, as we're done here
			if ibf.cache.offset >= ibf.dstBS {
				pair := &blockIndexPair{
					Block: ibf.cache.output,
					Index: ibf.cache.prevIndex / ibf.ratio,
				}

				ibf.newOutputBlock(blockPair)
				return pair, nil
			}
		}

		// remember the prev index for the next cycle (if there is one)
		ibf.cache.prevIndex = blockPair.Index

		// copy the fetched block into our final destination block
		copy(ibf.cache.output[ibf.cache.offset:ibf.cache.offset+ibf.srcBS], blockPair.Block)
		ibf.cache.offset += ibf.srcBS
	}

	// return a filled destination block
	pair := &blockIndexPair{
		Block: ibf.cache.output,
		Index: ibf.cache.prevIndex / ibf.ratio,
	}
	ibf.cache.output = nil
	return pair, nil
}

func (ibf *inflationBlockFetcher) newOutputBlock(pair *blockIndexPair) {
	// create output
	ibf.cache.output = make([]byte, ibf.dstBS)

	// ensure that we start at the correct local offset
	ibf.cache.offset = (pair.Index % ibf.ratio) * ibf.srcBS
	// store the prevIndex, so we can use it for the next cycles
	ibf.cache.prevIndex = pair.Index

	// copy the fetched block into our final destination block
	copy(ibf.cache.output[ibf.cache.offset:ibf.cache.offset+ibf.srcBS], pair.Block)
	ibf.cache.offset += ibf.srcBS
}

// newDeflationBlockFetcher creates a new Deflation BlockFetcher,
// wrapping around the given block fetcher.
// See `inflationBlockFetcher` for more information.
func newDeflationBlockFetcher(bf blockFetcher, srcBS, dstBS int64) *deflationBlockFetcher {
	return &deflationBlockFetcher{
		in:    bf,
		srcBS: srcBS,
		dstBS: dstBS,
		ratio: srcBS / dstBS,
		cb:    nil,
		cbi:   -1,
	}
}

// deflationBlockFetcher allows you to fetch smaller blocks,
// from an internal blockFetcher which itself returns bigger blocks.
type deflationBlockFetcher struct {
	in           blockFetcher
	srcBS, dstBS int64
	ratio        int64

	// current block
	cb  []byte // data
	cbi int64  // index
}

// FetchBlock implements blockFetcher.FetchBlock
func (dbf *deflationBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	var block []byte

	// continue fetching until we have a non-nil block
	for {
		for len(dbf.cb) > 0 {
			// continue distributing the already fetched block
			block = dbf.cb[:dbf.dstBS]
			// progress the cached slice
			dbf.cb = dbf.cb[dbf.dstBS:]

			if !isNilBlock(block) {
				// return the sliced block
				pair := &blockIndexPair{
					Block: block,
					Index: dbf.cbi,
				}
				dbf.cbi++
				return pair, nil
			}

			dbf.cbi++
		}

		// get next block, and recurse call this function,
		// such that we return the first part
		pair, err := dbf.in.FetchBlock()
		if err != nil {
			return nil, err
		}

		dbf.cb = pair.Block
		dbf.cbi = pair.Index * dbf.ratio
	}
}

func newStreamBlockFetcher() *streamBlockFetcher {
	return &streamBlockFetcher{
		sequences: make(map[int64]blockIndexPair),
	}
}

// streamBlockFetcher is a specialized blockFetcher,
// which is only supposed to be used for the places where blocks come as an unordered stream.
// This fetcher allows you to fetch the blocks in an ordered fashion,
// by keeping a local cursor to track progress and caching the out-of-order blocks.
type streamBlockFetcher struct {
	sequences     map[int64]blockIndexPair
	scursor       int64
	streamStopped bool
}

// FetchBlock implements blockFetcher.FetchBlock
func (sbf *streamBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	if sbf.streamStopped && len(sbf.sequences) == 0 {
		return nil, io.EOF
	}

	pair, ok := sbf.sequences[sbf.scursor]
	if !ok {
		return nil, errStreamBlocked
	}

	delete(sbf.sequences, sbf.scursor)
	sbf.scursor++
	return &pair, nil
}

// isNilBlock returns true if the given block contains only 0.
func isNilBlock(block []byte) bool {
	for _, b := range block {
		if b != 0 {
			return false
		}
	}

	return true
}

// Create a block storage ready for importing/exporting to/from a backup.
func createStorageConfig(vdiskID string, sourceConfig config.SourceConfig) (*storageConfig, error) {
	storageConfigCloser, err := config.NewSource(sourceConfig)
	if err != nil {
		return nil, err
	}
	defer storageConfigCloser.Close()

	vdiskConfig, err := config.ReadVdiskStaticConfig(storageConfigCloser, vdiskID)
	if err != nil {
		return nil, err
	}

	nbdStorageConfig, err := config.ReadNBDStorageConfig(storageConfigCloser, vdiskID)
	if err != nil {
		return nil, err
	}

	return &storageConfig{
		NBD:   *nbdStorageConfig,
		Vdisk: *vdiskConfig,
	}, nil
}

var (
	errNilVdiskID       = errors.New("vdisk's identifier not given")
	errInvalidCryptoKey = errors.New("invalid crypto key")
	errStreamBlocked    = errors.New("stream block fetcher is blocked waiting for next expected block")
)
