package backup

import (
	"bytes"
	"context"
	"io"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

// Export a block storage to aa FTP Server,
// in a secure and space efficient manner,
// in order to provide a backup (snapshot) for later usage.
func Export(ctx context.Context, cfg Config) error {
	err := cfg.validate()
	if err != nil {
		return err
	}

	log.Debugf("collecting all stored block indices for vdisk %s, this might take a while...", cfg.VdiskID)
	indices, err := storage.ListBlockIndices(cfg.VdiskID, cfg.ConfigSource)
	if err != nil {
		return errors.Wrapf(err,
			"couldn't list block (storage) indices (does vdisk '%s' exist?)",
			cfg.VdiskID)
	}

	pool := ardb.NewPool(nil)
	defer pool.Close()

	blockStorage, err := storage.BlockStorageFromConfig(
		cfg.VdiskID,
		cfg.ConfigSource,
		pool)
	if err != nil {
		return err
	}
	defer blockStorage.Close()

	storageDriver, err := newStorageDriver(cfg.BackupStoragDriverConfig)
	if err != nil {
		return err
	}
	defer storageDriver.Close()

	staticConfig, err := config.ReadVdiskStaticConfig(cfg.ConfigSource, cfg.VdiskID)
	if err != nil {
		return err
	}

	exportConfig := exportConfig{
		JobCount:        cfg.JobCount,
		SrcBlockSize:    int64(staticConfig.BlockSize),
		DstBlockSize:    cfg.BlockSize,
		VdiskSize:       staticConfig.Size * 1024 * 1024 * 1024, // GiB -> bytes
		CompressionType: cfg.CompressionType,
		CryptoKey:       cfg.CryptoKey,
		VdiskID:         cfg.VdiskID,
		SnapshotID:      cfg.SnapshotID,
		Force:           cfg.Force,
	}

	return exportBS(ctx, blockStorage, indices, storageDriver, exportConfig)
}

// existingOrNewHeader tries to first fetch an existing (snapshot) header from a given server,
// if it doesn't exist yet, a new one will be created in-memory instead.
// If it did exist already, it will be optionally decrypted, decompressed and loaded in-memory as a Header.
// When `cfg.Force` is `true`, a new header will be created, even if one existed already but couldn't be loaded.
// When `cfg.Force` is `false`, and a header exists but can't be a loaded,
// the error of why it couldn't be loaded, is returned instead.
func existingOrNewHeader(cfg exportConfig, src StorageDriver, key *CryptoKey, ct CompressionType) (*Header, error) {
	header, err := LoadHeader(cfg.SnapshotID, src, key, ct)

	if errors.Cause(err) == ErrDataDidNotExist {
		// deduped map did not exist yet,
		// return a new one based on the given export config
		return newExportHeader(cfg), nil
	}
	if err != nil {
		// deduped map did exist, but we couldn't load it.
		if cfg.Force {
			// we forcefully create a new one anyhow if `force == true`
			log.Debugf(
				"couldn't read header for snapshot '%s' due to an error (%s), forcefully creating a new one",
				cfg.SnapshotID, err)
			return newExportHeader(cfg), nil
		}
		// deduped map did exist,
		// but an error was triggered while fetching it
		return nil, err
	}

	if header.Metadata.BlockSize != cfg.DstBlockSize {
		if cfg.Force {
			// we forcefully create a new one anyhow if `force == true`
			log.Debugf(
				"existing header for snapshot '%s' defined incompatible snapshot blocksize, forcefully creating a new one",
				cfg.SnapshotID)
			return newExportHeader(cfg), nil
		}

		return nil, errIncompatibleHeader
	}

	// update information to match new export session
	header.Metadata.Created = time.Now().Format(time.RFC3339)
	header.Metadata.Source.VdiskID = cfg.VdiskID
	header.Metadata.Source.BlockSize = cfg.SrcBlockSize
	header.Metadata.Source.Size = int64(cfg.VdiskSize)
	header.Metadata.Version = zerodisk.CurrentVersion

	// return existing header, which was updated
	log.Debugf("loaded and updated existing header for snapshot %s", cfg.SnapshotID)
	return header, nil
}

var (
	errIncompatibleHeader = errors.New("incompatible snapshot header")
)

func newExportHeader(cfg exportConfig) *Header {
	return &Header{
		Metadata: Metadata{
			SnapshotID: cfg.SnapshotID,
			BlockSize:  cfg.DstBlockSize,
			Created:    time.Now().Format(time.RFC3339),
			Source: Source{
				VdiskID:   cfg.VdiskID,
				BlockSize: cfg.SrcBlockSize,
				Size:      int64(cfg.VdiskSize),
			},
			Version: zerodisk.CurrentVersion,
		},
		DedupedMap: RawDedupedMap{},
	}
}

func exportBS(ctx context.Context, src storage.BlockStorage, blockIndices []int64, dst StorageDriver, cfg exportConfig) error {
	// load the header, or create a new one if it doesn't exist yet
	header, err := existingOrNewHeader(cfg, dst, &cfg.CryptoKey, cfg.CompressionType)
	if err != nil {
		return err
	}
	// unpack the raw deduped map so we can use it as the model we require it to be
	dedupedMap, err := unpackRawDedupedMap(header.DedupedMap)
	if err != nil {
		return err
	}

	// setup the errgroup context that we'll use for all worker goroutines
	group, ctx := errgroup.WithContext(ctx)

	// used to send all the indices to, one by one
	indexCh := make(chan sequenceBlockIndexPair, cfg.JobCount) // gets closed by index fetch goroutine
	// used to fetch all storage blocks into,
	// so that they can be sent to the inputCh in an ordered fashion
	glueCh := make(chan exportGlueInput, cfg.JobCount) // gets closed when all block storage content is fetched
	// used as input for the compress->encrypt->write pipelines (goroutines)
	inputCh := make(chan blockIndexPair, cfg.JobCount) // gets closed by glue goroutine)

	// launch index sender,
	// such that we can fetch source blocks in parallel
	group.Go(func() error {
		log.Debug("starting export's source-block's index sender")
		defer close(indexCh)
		defer log.Debug("stopping export's source-block's index sender")

		var pair sequenceBlockIndexPair
		for sequenceIndex, blockIndex := range blockIndices {
			pair.SequenceIndex = int64(sequenceIndex)
			pair.BlockIndex = blockIndex
			select {
			case indexCh <- pair:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})

	// launch all fetchers, so it can start fetching blocks
	for i := 0; i < cfg.JobCount; i++ {
		id := i
		group.Go(func() error {
			log.Debug("starting export's block fetcher #", id)

			var err error
			defer func() {
				if err != nil {
					log.Errorf("stopping export's block fetcher #%d with error: %v", id, err)
					return
				}
				log.Debug("stopping export's block fetcher #", id)
			}()

			var open bool
			var input sequenceBlockIndexPair
			var output exportGlueInput

			for {
				select {
				case input, open = <-indexCh:
					if !open {
						// when indexCh is closed,
						// send a last item over the channel,
						// as to indicate this goroutine is finished
						select {
						case glueCh <- exportGlueInput{BlockIndex: -1}:
						case <-ctx.Done():
						}
						return nil
					}

					output.BlockData, err = src.GetBlock(input.BlockIndex)
					if err != nil {
						return err
					}

					output.BlockIndex = input.BlockIndex
					output.SequenceIndex = input.SequenceIndex

					select {
					case glueCh <- output:
					case <-ctx.Done():
						return nil
					}

				case <-ctx.Done():
					return nil
				}
			}
		})
	}

	// launch glue goroutine,
	// which sizes all blocks to the correct size,
	// and which ensures all blocks are in order as indicated by the blockIndices input slice.
	group.Go(func() (err error) {
		log.Debug("starting export's glue goroutine")
		defer close(inputCh)

		defer func() {
			if err != nil {
				log.Errorf("stopping export's glue goroutine with error: %v", err)
				return
			}
			log.Debug("stopping export's glue goroutine")
		}()

		sbf := newStreamBlockFetcher()
		obf := sizedBlockFetcher(sbf, cfg.SrcBlockSize, cfg.DstBlockSize)

		defer func() {
			if err != nil {
				return
			}

			// if no error has yet occured,
			// ensure that at the end of this function,
			// the block fetcher is empty
			_, err = obf.FetchBlock()
			if errors.Cause(err) == io.EOF {
				err = nil
			} else if err != nil {
				err = errors.Wrap(err, "export glue gouroutine's block fetcher still has unprocessed content left")
			}
		}()

		var closedExportChannels int
		var input exportGlueInput
		var pair *blockIndexPair
		var open bool

		for {
			select {
			case input, open = <-glueCh:
				if open && input.BlockIndex != -1 {
					if input.SequenceIndex < sbf.scursor {
						// NOTE: this should never happen,
						//       as it indicates a bug in the code
						panic(errors.Newf(
							"unexpected sequence index returned, received %d, which is lower then %d",
							input.SequenceIndex,
							sbf.scursor,
						))
					}

					// cache the current received output
					sbf.sequences[input.SequenceIndex] = blockIndexPair{
						Block: input.BlockData,
						Index: input.BlockIndex,
					}

					if input.SequenceIndex > sbf.scursor {
						// we received an out-of-order index,
						// so wait for the next one
						continue
					}
				} else if input.BlockIndex == -1 {
					closedExportChannels++
					if closedExportChannels >= cfg.JobCount {
						sbf.streamStopped = true
						open = false
					}
				} else {
					sbf.streamStopped = true
				}

				// sequenceIndex == scursor
				// continue processing as much blocks as possible,
				// with the current cached output
				for {
					pair, err = obf.FetchBlock()
					if err != nil {
						cause := errors.Cause(err)
						if cause == io.EOF || cause == errStreamBlocked {
							err = nil
							break // we have nothing more to send (for now)
						}
						// unknown error, quit!
						return err
					}

					// send block for storage
					select {
					case inputCh <- *pair:
					case <-ctx.Done():
						return nil
					}
				}

				if !open {
					return nil
				}

			case <-ctx.Done():
				return nil
			}
		}
	})

	// launch all pipeline workers
	for i := 0; i < cfg.JobCount; i++ {
		compressor, err := NewCompressor(cfg.CompressionType)
		if err != nil {
			return err
		}

		var encrypter Encrypter
		if cfg.CryptoKey.Defined() {
			encrypter, err = NewEncrypter(&cfg.CryptoKey)
			if err != nil {
				return err
			}
		}

		hasher, err := newKeyedHasher(cfg.CompressionType, cfg.CryptoKey)
		if err != nil {
			return err
		}

		pipeline := &exportPipeline{
			Hasher:        hasher,
			Compressor:    compressor,
			Encrypter:     encrypter,
			StorageDriver: dst,
			DedupedMap:    dedupedMap,
		}

		// launch worker
		id := i
		group.Go(func() error {
			log.Debugf("starting export pipeline worker #%d", id)

			var err error
			defer func() {
				if err != nil {
					log.Errorf("stopping export pipeline worker #%d with error: %v", id, err)
					return
				}
				log.Debugf("stopping export pipeline worker #%d", id)
			}()

			var input blockIndexPair
			var open bool

			for {
				select {
				case input, open = <-inputCh:
					if !open {
						return nil
					}

					err = pipeline.WriteBlock(input.Index, input.Block)
					if err != nil {
						return errors.Wrap(err, "error while processing block")
					}

				case <-ctx.Done():
					return nil
				}
			}
		})
	}

	// check if error was thrown, if so, quit with an error immediately
	exportErr := group.Wait()
	if exportErr != nil {
		return exportErr
	}

	// get the raw deduped map, so the header can be prepared and stored as well
	RawDedupedMap, err := dedupedMap.Raw()
	if err != nil {
		return err
	}
	header.DedupedMap = *RawDedupedMap

	// store the (updated) header
	return StoreHeader(header, &cfg.CryptoKey, cfg.CompressionType, dst)
}

// used to connect a sequence index to a block index,
// such that we can order blocks fetched in parallel at a later time.
type sequenceBlockIndexPair struct {
	SequenceIndex int64
	BlockIndex    int64
}

type exportGlueInput struct {
	BlockIndex    int64
	SequenceIndex int64
	BlockData     []byte
}

type exportConfig struct {
	JobCount int

	SrcBlockSize int64
	DstBlockSize int64

	VdiskSize uint64

	CompressionType CompressionType
	CryptoKey       CryptoKey

	VdiskID    string
	SnapshotID string

	Force bool
}

// compress -> encrypt -> store
type exportPipeline struct {
	Hasher        zerodisk.Hasher
	Compressor    Compressor
	Encrypter     Encrypter
	StorageDriver StorageDriver
	DedupedMap    *dedupedMap
}

func (p *exportPipeline) WriteBlock(index int64, data []byte) error {
	bufA := bytes.NewBuffer(data)
	bufB := bytes.NewBuffer(nil)

	hash := p.Hasher.HashBytes(bufA.Bytes())
	blockIsNew := p.DedupedMap.SetHash(index, hash)
	if !blockIsNew {
		return nil // we're done here
	}

	if p.Encrypter != nil {
		// compress and encrypt
		err := p.Compressor.Compress(bufA, bufB)
		if err != nil {
			return err
		}

		bufA = bytes.NewBuffer(nil)
		err = p.Encrypter.Encrypt(bufB, bufA)
		if err != nil {
			return err
		}
	} else {
		// compress
		err := p.Compressor.Compress(bufA, bufB)
		if err != nil {
			return err
		}
		bufA = bufB
	}

	return p.StorageDriver.SetDedupedBlock(hash, bufA)
}
