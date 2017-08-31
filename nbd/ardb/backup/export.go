package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/zero-os/0-Disk"
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

	storageConfig, err := createBlockStorage(cfg.VdiskID, cfg.BlockStorageConfig, true)
	if err != nil {
		return err
	}

	ardbProvider, err := ardb.StaticProvider(storageConfig.NBD, nil)
	if err != nil {
		return err
	}
	defer ardbProvider.Close()

	blockStorage, err := storage.NewBlockStorage(storageConfig.BlockStorage, ardbProvider)
	if err != nil {
		return err
	}
	defer blockStorage.Close()

	storageDriver, err := NewStorageDriver(cfg.BackupStorageConfig)
	if err != nil {
		return err
	}
	defer storageDriver.Close()

	exportConfig := exportConfig{
		JobCount:        cfg.JobCount,
		SrcBlockSize:    storageConfig.BlockStorage.BlockSize,
		DstBlockSize:    cfg.BlockSize,
		CompressionType: cfg.CompressionType,
		CryptoKey:       cfg.CryptoKey,
		SnapshotID:      cfg.SnapshotID,
		Force:           cfg.Force,
	}

	return exportBS(ctx, blockStorage, storageConfig.Indices, storageDriver, exportConfig)
}

func exportBS(ctx context.Context, src storage.BlockStorage, blockIndices []int64, dst StorageDriver, cfg exportConfig) error {
	// load the deduped map, or create a new one if it doesn't exist yet
	dedupedMap, err := ExistingOrNewDedupedMap(
		cfg.SnapshotID, dst, &cfg.CryptoKey, cfg.CompressionType, cfg.Force)
	if err != nil {
		return err
	}

	// setup the context that we'll use for all worker goroutines
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// used to send all the indices to, one by one
	indexCh := make(chan sequenceBlockIndexPair, cfg.JobCount) // gets closed by index fetch goroutine
	// used to fetch all storage blocks into,
	// so that they can be sent to the inputCh in an ordered fashion
	glueCh := make(chan exportGlueInput, cfg.JobCount) // gets closed when all block storage content is fetched
	// used as input for the compress->encrypt->write pipelines (goroutines)
	inputCh := make(chan blockIndexPair, cfg.JobCount) // gets closed by glue goroutine

	errCh := make(chan error)
	defer close(errCh)

	sendErr := func(err error) {
		log.Errorf("an error occured while exporting: %v", err)
		select {
		case errCh <- err:
		default:
		}
	}

	// launch index sender,
	// such that we can fetch source blocks in parallel
	go func() {
		log.Debug("starting export's source-block's index sender")
		defer close(indexCh)
		defer log.Debug("stopping export's source-block's index sender")

		for sequenceIndex, blockIndex := range blockIndices {
			indexCh <- sequenceBlockIndexPair{
				SequenceIndex: int64(sequenceIndex),
				BlockIndex:    blockIndex,
			}
		}
	}()

	var exportErr error
	// err ch used to
	go func() {
		select {
		case <-ctx.Done():
		case exportErr = <-errCh:
			cancel() // stop all other goroutines
		}
	}()

	// input wait group
	var iwg sync.WaitGroup

	// launch all fetchers, so it can start fetching blocks
	iwg.Add(cfg.JobCount)
	for i := 0; i < cfg.JobCount; i++ {
		go func(id int) {
			log.Debug("starting export's block fetcher #", id)
			defer iwg.Done()

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
				case <-ctx.Done():
					return

				case input, open = <-indexCh:
					if !open {
						return
					}

					output.BlockData, err = src.GetBlock(input.BlockIndex)
					if err != nil {
						sendErr(err)
						return
					}

					output.BlockIndex = input.BlockIndex
					output.SequenceIndex = input.SequenceIndex

					select {
					case <-ctx.Done():
						return
					case glueCh <- output:
					}
				}
			}
		}(i)
	}

	// launch glue goroutine,
	// which sizes all blocks to the correct size,
	// and which ensures all blocks are in order as indicated by the blockIndices input slice.
	go func() {
		log.Debug("starting export's glue goroutine")
		defer close(inputCh)

		var err error
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
			if err == nil || err != io.EOF {
				err = errors.New("export glue gouroutine's block fetcher still has unprocessed content left")
				sendErr(err)
				return
			}
			err = nil
		}()

		var input exportGlueInput
		var pair *blockIndexPair
		var open bool

		for {
			select {
			case <-ctx.Done():
			case input, open = <-glueCh:
				if open {
					if input.SequenceIndex < sbf.scursor {
						// NOTE: this should never happen,
						//       as it indicates a bug in the code
						err = fmt.Errorf(
							"unexpected sequence index returned, received %d, which is lower then %d",
							input.SequenceIndex, sbf.scursor)
						sendErr(err)
						return
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
				} else {
					sbf.streamStopped = true
				}

				// sequenceIndex == scursor
				// continue processing as much blocks as possible,
				// with the current cached output
				for {
					pair, err = obf.FetchBlock()
					if err != nil {
						if err == io.EOF || err == errStreamBlocked {
							err = nil
							break // we have nothing more to send (for now)
						}
						// unknown error, quit!
						sendErr(err)
						return
					}

					// send block for storage
					select {
					case <-ctx.Done():
						return
					case inputCh <- *pair:
					}
				}

				if !open {
					return
				}
			}
		}
	}()

	// output wait group
	var owg sync.WaitGroup

	// launch all pipeline workers
	owg.Add(cfg.JobCount)
	for i := 0; i < cfg.JobCount; i++ {
		compressor, err := NewCompressor(cfg.CompressionType)
		if err != nil {
			return err
		}
		encrypter, err := NewEncrypter(&cfg.CryptoKey)
		if err != nil {
			return err
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
		go func(id int) {
			defer owg.Done()

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
				case <-ctx.Done():
					return

				case input, open = <-inputCh:
					if !open {
						return
					}

					err = pipeline.WriteBlock(input.Index, input.Block)
					if err != nil {
						sendErr(fmt.Errorf("error while processing block: %v", err))
						return
					}
				}
			}
		}(i)
	}

	// wait until all blocks have been fetched and backed up
	iwg.Wait()
	close(glueCh)
	owg.Wait()

	// check if error was thrown, if so, quit with an error immediately
	if exportErr != nil {
		return exportErr
	}

	// store the deduped map
	buf := bytes.NewBuffer(nil)
	err = dedupedMap.Serialize(&cfg.CryptoKey, cfg.CompressionType, buf)
	if err != nil {
		return err
	}
	return dst.SetDedupedMap(cfg.SnapshotID, buf)
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

	CompressionType CompressionType
	CryptoKey       CryptoKey

	SnapshotID string

	Force bool
}

// compress -> encrypt -> store
type exportPipeline struct {
	Hasher        zerodisk.Hasher
	Compressor    Compressor
	Encrypter     Encrypter
	StorageDriver StorageDriver
	DedupedMap    *DedupedMap
}

func (p *exportPipeline) WriteBlock(index int64, data []byte) error {
	bufA := bytes.NewBuffer(data)
	bufB := bytes.NewBuffer(nil)

	hash := p.Hasher.HashBytes(bufA.Bytes())
	blockIsNew := p.DedupedMap.SetHash(index, hash)
	if !blockIsNew {
		return nil // we're done here
	}

	err := p.Compressor.Compress(bufA, bufB)
	if err != nil {
		return err
	}

	bufA = bytes.NewBuffer(nil)
	err = p.Encrypter.Encrypt(bufB, bufA)
	if err != nil {
		return err
	}

	return p.StorageDriver.SetDedupedBlock(hash, bufA)
}
