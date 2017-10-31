package tlog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient"
)

// Storage creates a tlog storage BlockStorage,
// wrapping around a given backend storage,
// using the given tlog client to send its write transactions to the tlog server.
func Storage(ctx context.Context, vdiskID string, configSource config.Source, blockSize int64, bstorage storage.BlockStorage, cluster ardb.StorageCluster, client tlogClient) (storage.BlockStorage, error) {
	if bstorage == nil {
		return nil, errors.New("tlogStorage requires a non-nil BlockStorage")
	}
	if cluster == nil {
		return nil, errors.New("tlogStorage requires a non-nil StorageCluster")
	}

	metadata, err := storage.LoadTlogMetadata(vdiskID, cluster)
	if err != nil {
		return nil, errors.New("tlogStorage requires a valid metadata ARDB provider: " + err.Error())
	}

	ctx, cancel := context.WithCancel(ctx)
	tlogStorage := &tlogStorage{
		vdiskID:       vdiskID,
		storage:       bstorage,
		metadata:      metadata,
		cluster:       cluster,
		cache:         newInMemorySequenceCache(),
		blockSize:     blockSize,
		transactionCh: make(chan *transaction, transactionChCapacity),
		toFlushCh:     make(chan []uint64, toFlushChCapacity),
		// there is no strong reason to choose 5 as the channel size.
		// i think we normally only need 1.
		// I give 5 just in case we need more than 1, because it is async
		// after all, a bit hard to predict.
		// On the other hand, 5 as channel size doesn't really matter
		// in term of memory usage
		flushErrCh:     make(chan error, 5),
		cacheEmptyCond: sync.NewCond(&sync.Mutex{}),
		cancel:         cancel,
	}

	tlogClusterConfig, err := tlogStorage.tlogRPCReloader(ctx, vdiskID, configSource)
	if err != nil {
		cancel()
		return nil, errors.New("tlogstorage couldn't retrieve data from config: " + err.Error())
	}

	if client == nil {
		client, err = tlogclient.New(tlogClusterConfig.Servers, vdiskID)
		if err != nil {
			cancel()
			return nil, errors.New("tlogStorage requires valid tlogclient: " + err.Error())
		}

		if metadata.LastFlushedSequence < client.LastFlushedSequence() {
			// TODO call tlog player if last flushed sequence from tlog server is
			// higher than us.
			// see: https://github.com/zero-os/0-Disk/issues/230
			log.Infof("possible error when starting vdisk `%v`: might need to sync with tlog", vdiskID)
		}
	}
	tlogStorage.tlog = client
	tlogStorage.sequence = client.LastFlushedSequence() + 1

	err = tlogStorage.spawnBackgroundGoroutine(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	return tlogStorage, nil
}

// tlogStorage is a BlockStorage implementation,
// which is a special storage type in the series of storage implementations.
// It doesn't store the actual data itself,
// and instead wraps around another storage (e.g. nondeduped/deduped storage),
// to do the actual data storage.
// Thus this wrapping storage isn't used to store data,
// and is instead used to send the tlogserver all write transactions,
// such that these are logged (e.g. for data recovery), using an embedded tlogclient.
type tlogStorage struct {
	vdiskID       string
	tlogClusterID string
	mux           sync.RWMutex
	tlog          tlogClient
	storage       storage.BlockStorage
	metadata      storage.TlogMetadata
	cluster       ardb.StorageCluster
	cache         sequenceCache
	blockSize     int64
	sequence      uint64
	sequenceMux   sync.Mutex
	transactionCh chan *transaction
	toFlushCh     chan []uint64 // channel of sequences to be flushed
	flushErrCh    chan error    // channel of all flush related errors

	// mutex for the ardb storage
	storageMux sync.Mutex

	// condition variable used to signal when the cache is empty
	cacheEmptyCond *sync.Cond

	cancel context.CancelFunc
}

type transaction struct {
	Operation uint8
	Sequence  uint64
	Content   []byte
	Index     int64
	Timestamp int64
}

// SetBlock implements BlockStorage.SetBlock
func (tls *tlogStorage) SetBlock(blockIndex int64, content []byte) error {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	return tls.set(blockIndex, content)
}

func (tls *tlogStorage) set(blockIndex int64, content []byte) error {
	var op uint8

	// define tlog opcode, content and length
	if content == nil {
		op = schema.OpDelete
	} else if tls.isZeroContent(content) {
		op = schema.OpDelete
		content = nil
	} else {
		op = schema.OpSet
	}

	// get next tlog sequence index,
	// never interact directly with tls.sequence
	// as this is not thread safe,
	// while any BlockStorage HAS to be threadsafe
	sequence := tls.getNextSequenceIndex()

	// Add the transaction to cache.
	//
	// it's pretty safe to have an invalid transaction in cache,
	// but it would be really bad if the server has a transaction,
	// which is not available in the cache,
	// as it would mean that the storage wouldn't have the content available,
	// even though it is stored in the replay server
	err := tls.cache.Add(sequence, blockIndex, content)
	if err != nil {
		return fmt.Errorf(
			"tlogStorage couldn't set content at block %d (seq: %d): %s",
			blockIndex, sequence, err)
	}

	// copy the content to avoid race condition with value in cache
	transactionContent := make([]byte, len(content))
	copy(transactionContent, content)

	// scheduele tlog transaction, to be sent to the server
	tls.transactionCh <- &transaction{
		Operation: op,
		Sequence:  sequence,
		Index:     blockIndex,
		Timestamp: tlog.TimeNowTimestamp(),
		Content:   transactionContent,
	}

	// success!
	// content is being processed by the tlog server (hopefully)
	// and it's waiting in the history cache, until the tlog server is done
	return nil
}

// GetBlock implements BlockStorage.GetBlock
func (tls *tlogStorage) GetBlock(blockIndex int64) (content []byte, err error) {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	content, err = tls.get(blockIndex)
	return
	/*
		// ARDB SLAVE SWAP IS DISABLED SINCE MILESTONE 6
		// SEE: https://github.com/zero-os/0-Disk/issues/357

		if err == nil {
			return
		}

		// there is issue with master, switch to ardb slave if possible
		if errSwitch := tls.switchToArdbSlave(); errSwitch != nil {
			log.Errorf("Failed to switchToArdbSlave: %v", errSwitch)
			return
		}

		return tls.get(blockIndex)
	*/
}

// DeleteBlock implements BlockStorage.DeleteBlock
func (tls *tlogStorage) DeleteBlock(blockIndex int64) (err error) {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	err = tls.set(blockIndex, nil)
	return
}

// Flush implements BlockStorage.Flush
func (tls *tlogStorage) Flush() error {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	// ForceFlush at the latest sequence
	latestSeq := tls.getLatestSequence()
	tls.tlog.ForceFlushAtSeq(latestSeq)

	// wait until the cache is empty or timeout
	doneCh := make(chan struct{})
	go func() {
		tls.cacheEmptyCond.L.Lock()
		if !tls.cache.Empty() {
			tls.cacheEmptyCond.Wait()
		}
		tls.cacheEmptyCond.L.Unlock()
		close(doneCh)
	}()

	var finished bool
	var forceFlushNum int
	for !finished {
		select {
		case err := <-tls.flushErrCh:
			return err
		case <-time.After(FlushWaitRetry): // retry it
			tls.tlog.ForceFlushAtSeq(latestSeq)

			forceFlushNum++

			if forceFlushNum >= FlushWaitRetryNum {
				// if reach max retry number
				// signal the condition variable anyway
				// to avoid the goroutine blocked forever
				tls.cacheEmptyCond.Signal()
				return errFlushTimeout
			}

		case <-doneCh:
			// wait returned
			finished = true
		}
	}

	// flush actual storage
	tls.storageMux.Lock()
	defer tls.storageMux.Unlock()

	err := tls.storage.Flush()
	if err != nil {
		log.Errorf("error while flushing internal blockStorage: %v", err)
		return err
	}

	// only if we succesfully flushed the storage,
	// will we flush our metadata, as to make sure the metadata is in sync
	// with the content stored in the internal block storage.
	err = storage.StoreTlogMetadata(tls.vdiskID, tls.cluster, tls.metadata)
	if err != nil {
		log.Errorf("error while flushing metadata: %v", err)
	}
	return err
}

// Close implements BlockStorage.Close
func (tls *tlogStorage) Close() (err error) {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	tls.storageMux.Lock()
	defer tls.storageMux.Unlock()

	log.Infof("tlog storage closed with cache empty = %v", tls.cache.Empty())
	tls.cancel()

	err = tls.storage.Close()
	if err != nil {
		log.Info("error while closing internal tlog's storage: ", err)
	}

	err = tls.tlog.Close()
	if err != nil {
		log.Info("error while closing internal tlog's client: ", err)
	}

	log.Infof("tlogStorage Closed with lastSequence = %v, cache empty = %v",
		tls.getLatestSequence(), tls.cache.Empty())

	return
}

func (tls *tlogStorage) spawnBackgroundGoroutine(parentCtx context.Context) error {
	ctx, cancelFunc := context.WithCancel(parentCtx)
	recvCh := tls.tlog.Recv()

	// used for sending our transactions
	go tls.transactionSender(ctx)

	// used for flushing from sequence cache
	go tls.flusher(ctx)

	go func() {
		log.Debug("background goroutine spawned for tlogstorage ", tls.vdiskID)

		defer func() {
			log.Infof("GoBackground exited for vdisk: %v", tls.vdiskID)
			/*err := tls.tlog.Close()
			if err != nil {
				log.Info("error while closing tlog client: ", err)
			}*/
		}()

		defer log.Debug("background goroutine exiting for tlogstorage ", tls.vdiskID)
		defer cancelFunc()

		for {
			select {
			case res := <-recvCh:
				if res.Err != nil {
					tls.flushErrCh <- fmt.Errorf(
						"tlog server resulted in error for vdisk %s: %s",
						tls.vdiskID, res.Err)
					continue
				}
				if res.Resp == nil {
					log.Info(
						"tlog server returned nil-response for vdisk: ", tls.vdiskID)
					continue
				}

				switch res.Resp.Status {
				case tlog.BlockStatusRecvOK:
					// nothing to do, logging would be way too verbose

				case tlog.BlockStatusForceFlushReceived:
					log.Debugf("vdisk %s's tlog server has received force flush message", tls.vdiskID)

				case tlog.BlockStatusFlushOK:
					tls.toFlushCh <- res.Resp.Sequences
				case tlog.BlockStatusWaitNbdSlaveSyncReceived:

				case tlog.BlockStatusFlushFailed:
					log.Errorf(
						"tlog server failed to flush for vdisk %s",
						tls.vdiskID)
				case tlog.BlockStatusDisconnected:
					log.Info("tlog connection disconnected")
				default:
					log.Errorf(
						"got unknown response type for vdisk %s: %s",
						tls.vdiskID, res.Resp.Status)
				}

			case <-parentCtx.Done():
				log.Info("tlogclient receiver should be finished by now")
				return
			}
		}
	}()

	return nil
}

// transactionSender is spawned by the `GoBackground` method,
// and is meant to send transactions to the tlog client,
// over a seperate goroutine
func (tls *tlogStorage) transactionSender(ctx context.Context) {
	for {
		select {
		case transaction := <-tls.transactionCh:
			err := tls.tlog.Send(
				transaction.Operation,
				transaction.Sequence,
				transaction.Index,
				transaction.Timestamp,
				transaction.Content,
			)
			if err != nil {
				tls.flushErrCh <- fmt.Errorf(
					"tlogStorage couldn't send block %d (seq: %d): %s",
					transaction.Index,
					transaction.Sequence,
					err,
				)
			}

		case <-ctx.Done():
			log.Debugf("transaction sender for tlogStorage %s aborting", tls.vdiskID)
			return
		}
	}
}

// flusher is spawned by the `GoBackground` method,
// and is meant to flush transaction from sequence cache to storage,
// over a separate goroutine
func (tls *tlogStorage) flusher(ctx context.Context) {
	for {
		select {
		case seqs := <-tls.toFlushCh:
			err := tls.flushCachedContent(seqs)
			if err != nil {
				tls.flushErrCh <- fmt.Errorf(
					"failed to write cached content into storage for vdisk %s: %s",
					tls.vdiskID, err)
			}

		case <-ctx.Done():
			log.Debugf("flusher for tlogStorage %s aborting", tls.vdiskID)
			return
		}
	}

}

func (tls *tlogStorage) tlogRPCReloader(ctx context.Context, vdiskID string, source config.Source) (*config.TlogClusterConfig, error) {
	ctx, cancel := context.WithCancel(ctx)
	vdiskNBDUpdate, err := config.WatchVdiskNBDConfig(ctx, source, vdiskID)
	if err != nil {
		cancel()
		return nil, err
	}

	vdiskNBDConfig := <-vdiskNBDUpdate
	tls.tlogClusterID = vdiskNBDConfig.TlogServerClusterID

	tlogCfgCtx, tlogCfgCancel := context.WithCancel(ctx)
	tlogClusterCfgUpdate, err := config.WatchTlogClusterConfig(tlogCfgCtx, source, vdiskNBDConfig.TlogServerClusterID)
	if err != nil {
		tlogCfgCancel()
		cancel()
		return nil, err
	}

	var tlogClusterConfig config.TlogClusterConfig
	select {
	case tlogClusterConfig = <-tlogClusterCfgUpdate:
	case <-ctx.Done():
		tlogCfgCancel()
		cancel()
		return nil, errors.New("tlogStorage requires active context")
	}

	go func() {
		defer func() {
			tlogCfgCancel()
			cancel()
		}()

		for {
			select {
			case vdiskNBDConfig, ok := <-vdiskNBDUpdate:
				// in case channel is closed
				// but ctx.Done() wasn't triggered yet
				if !ok {
					return
				}

				if tls.tlogClusterID != vdiskNBDConfig.TlogServerClusterID {
					if vdiskNBDConfig.TlogServerClusterID == "" {
						source.MarkInvalidKey(config.Key{ID: vdiskID, Type: config.KeyVdiskNBD}, vdiskID)
						log.Errorf(
							"vdisk %s encountered an error switching to new Tlog cluster ID: %s",
							tls.vdiskID,
							"TlogServerClusterID is empty")
						continue
					}

					tlogCfgCtx, newTlogCfgCancel := context.WithCancel(ctx)
					newTlogClusterCfgUpdate, err := config.WatchTlogClusterConfig(tlogCfgCtx, source, vdiskNBDConfig.TlogServerClusterID)
					if err != nil {
						newTlogCfgCancel()
						log.Errorf(
							"vdisk %s encountered an error switching to Tlog cluster ID '%s': %s",
							tls.vdiskID,
							vdiskNBDConfig.TlogServerClusterID,
							err)
						continue
					}

					log.Debugf("Setting Tlog cluster ID to: %s", vdiskNBDConfig.TlogServerClusterID)
					tlogCfgCancel()
					tlogCfgCancel = newTlogCfgCancel
					tls.tlogClusterID = vdiskNBDConfig.TlogServerClusterID
					tlogClusterCfgUpdate = newTlogClusterCfgUpdate
				}
			case tlogClusterCfg := <-tlogClusterCfgUpdate:
				tls.tlog.ChangeServerAddresses(tlogClusterCfg.Servers)
			case <-ctx.Done():
				return
			}
		}
	}()

	return &tlogClusterConfig, nil
}

// switch to ardb slave if possible
//
// Disabled since Milestone 6!
// See: https://github.com/zero-os/0-Disk/issues/357
/*func (tls *tlogStorage) switchToArdbSlave() error {
	if tls.configPath == "" {
		return fmt.Errorf("no config found")
	}

	// get config file permission
	// we need it because we want to rewrite it.
	// better to write it with same permission
	filePerm, err := func() (os.FileMode, error) {
		info, err := os.Stat(tls.configPath)
		if err != nil {
			return 0, err
		}
		return info.Mode(), nil
	}()
	if err != nil {
		return err
	}

	// parse the config
	conf, err := config.ReadConfig(tls.configPath, config.NBDServer)
	if err != nil {
		return err
	}

	vdiskConf, exist := conf.Vdisks[tls.vdiskID]
	if !exist {
		return fmt.Errorf("no config found for vdisk: %v", tls.vdiskID)
	}

	if vdiskConf.SlaveStorageCluster == "" {
		return fmt.Errorf("vdisk %v doesn't have ardb slave", tls.vdiskID)
	}

	// switch to slave (still in config only)
	vdiskConf.StorageCluster = vdiskConf.SlaveStorageCluster
	vdiskConf.SlaveStorageCluster = ""
	conf.Vdisks[tls.vdiskID] = vdiskConf

	// ask tlog to sync the slave
	if err := tls.tlog.WaitNbdSlaveSync(); err != nil {
		return err
	}

	// rewrite the config
	log.Debug("using slave and rewriting config: ", tls.configPath)
	if err := ioutil.WriteFile(tls.configPath, []byte(conf.String()), filePerm); err != nil {
		return err
	}

	// reload the config
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)

	// give some time for the reloader to work
	// TODO : give access to the reloader, so we don't need to do some random sleep
	time.Sleep(1 * time.Second)

	// TODO : notify the orchestrator or ays or any other interested parties
	return nil
}*/

func (tls *tlogStorage) flushCachedContent(sequences []uint64) error {
	defer func() {
		tls.cacheEmptyCond.L.Lock()
		if tls.cache.Empty() {
			tls.cacheEmptyCond.Signal()
		}
		tls.cacheEmptyCond.L.Unlock()
	}()

	if len(sequences) == 0 {
		return nil // no work to do
	}

	tls.storageMux.Lock()
	defer tls.storageMux.Unlock()

	elements := tls.cache.Evict(sequences...)

	var errs flushErrors
	for blockIndex, data := range elements {
		err := tls.flushAContent(blockIndex, data)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs
	}

	// the sequences are given in ordered form,
	// thus we can simply try to set the last given sequence.
	tls.metadata.LastFlushedSequence = sequences[len(sequences)-1]
	return nil
}

func (tls *tlogStorage) flushAContent(blockIndex int64, data []byte) error {
	flush := func() error {
		if data == nil {
			return tls.storage.DeleteBlock(blockIndex)
		}
		return tls.storage.SetBlock(blockIndex, data)
	}

	/*
		// SWITCH ARDB SLAVE DISABLED SINCE MILESTONE 6
		// SEE: https://github.com/zero-os/0-Disk/issues/357

		err := flush()
		if err == nil {
			return nil
		}

		// there is issue with master, switch to ardb slave if possible
		if errSwitch := tls.switchToArdbSlave(); errSwitch != nil {
			log.Errorf("Failed to switchToArdbSlave: %v", errSwitch)
			return err
		}
	*/

	return flush()
}

type flushErrors []error

func (err flushErrors) Error() string {
	l := len(err)
	if l == 0 {
		return "unknown flush error"
	}

	var msg string

	l--
	for i := 0; i < l; i++ {
		msg += err[i].Error() + "; "
	}
	msg += err[l].Error()

	return msg
}

// get content from either cache, or internal storage
func (tls *tlogStorage) get(blockIndex int64) (content []byte, err error) {
	content, found := tls.cache.Get(blockIndex)
	if found {
		return
	}

	tls.storageMux.Lock()
	defer tls.storageMux.Unlock()

	// return content from internal storage if possible
	content, err = tls.storage.GetBlock(blockIndex)
	return
}

// isZeroContent detects if a given content buffer is completely filled with 0s
func (tls *tlogStorage) isZeroContent(content []byte) bool {
	for _, c := range content {
		if c != 0 {
			return false
		}
	}

	return true
}

// get the next sequence index,
// and increment if such that the next time another one can be retrieved
func (tls *tlogStorage) getNextSequenceIndex() (sequence uint64) {
	tls.sequenceMux.Lock()
	defer tls.sequenceMux.Unlock()
	sequence = tls.sequence
	tls.sequence++
	return
}

// get the latest transaction sequence
func (tls *tlogStorage) getLatestSequence() (sequence uint64) {
	tls.sequenceMux.Lock()
	defer tls.sequenceMux.Unlock()
	return tls.sequence - 1
}

// tlogClient represents the tlog client interface used
// by the tlogStorage, usually filled by (*tlogclient.Client)
//
// using an interface makes it possible to use a dummy version
// for testing purposes
type tlogClient interface {
	Send(op uint8, seq uint64, index int64, timestamp int64, data []byte) error
	ForceFlushAtSeq(uint64) error
	WaitNbdSlaveSync() error
	ChangeServerAddresses([]string)
	Recv() <-chan *tlogclient.Result
	LastFlushedSequence() uint64
	Close() error
}

// sequenceCache, is used to cache transactions, linked to their sequenceIndex,
// which are still being processed by the tlogServer, and thus awaiting to be flushed.
// A sequenceCache HAS to be thread-safe.
type sequenceCache interface {
	// Add a transaction to the cache
	Add(sequenceIndex uint64, blockIndex int64, data []byte) error
	// Get the latest version of a transaction from the cache
	Get(blockIndex int64) ([]byte, bool)
	// Evict all transactions for the given sequences
	Evict(sequences ...uint64) map[int64][]byte
	// Empty returns true if the cache has no cached transactions
	Empty() bool
}

// newInMemorySequenceCache creates a new in-memory sequence cache
func newInMemorySequenceCache() *inMemorySequenceCache {
	return &inMemorySequenceCache{
		sequences: make(map[uint64]int64),
		values:    make(map[int64]*dataHistory),
	}
}

// inMemorySequenceCache is used as the in-memory sequence cache,
// for all transactions which the tlogserver is still processing.
type inMemorySequenceCache struct {
	// mapping between sequenceIndex and blockIndex
	sequences map[uint64]int64
	// mapping between blockIndex and data
	values map[int64]*dataHistory
	// size of history blocks
	size uint64

	mux sync.RWMutex
}

// Add implements sequenceCache.Add
func (sq *inMemorySequenceCache) Add(sequenceIndex uint64, blockIndex int64, data []byte) error {
	sq.mux.Lock()
	defer sq.mux.Unlock()

	// store the actual data
	dh, ok := sq.values[blockIndex]
	if !ok {
		dh = newDataHistory()
		sq.values[blockIndex] = dh
		sq.size++
	}
	if err := dh.Add(sequenceIndex, data); err != nil {
		if err == errOutdated {
			if log.GetLevel() == log.DebugLevel {
				latest, _ := dh.LatestSequence()
				log.Debugf(
					"not overwriting data (seq %d) of block %d: for seq %d",
					latest,
					blockIndex,
					sequenceIndex)
			}
			return nil
		}

		return nil
	}

	// map sequence- to block index
	sq.sequences[sequenceIndex] = blockIndex

	return nil
}

// Get implements sequenceCache.Get
func (sq *inMemorySequenceCache) Get(blockIndex int64) ([]byte, bool) {
	sq.mux.RLock()
	defer sq.mux.RUnlock()

	dh, ok := sq.values[blockIndex]
	if !ok {
		return nil, false
	}

	return dh.Latest()
}

// Evict implements sequenceCache.Evict
func (sq *inMemorySequenceCache) Evict(sequences ...uint64) (elements map[int64][]byte) {
	sq.mux.Lock()
	defer sq.mux.Unlock()

	elements = make(map[int64][]byte)

	var ok bool
	var element []byte
	var dh *dataHistory
	var blockIndex int64

	for _, sequenceIndex := range sequences {
		// remove sequenceIndex
		blockIndex, ok = sq.sequences[sequenceIndex]
		if !ok {
			log.Debug("tried to evict invalid (outdated?) blockIndex: ", sequenceIndex)
			continue
		}
		delete(sq.sequences, sequenceIndex)

		dh, ok = sq.values[blockIndex]
		if !ok {
			// could be because sequence was already deleted
			// in the dataHistory, and that because of that
			// and perhaps other deletes,
			// the history got empty and thus deleted.
			continue
		}

		element, ok = dh.Trim(sequenceIndex)
		if !ok {
			// could be because sequence was already deleted
			// which is possible due to a previous mass-trim
			continue
		}

		// add the found element
		elements[blockIndex] = element

		// delete the history object, in case there are no values left
		if dh.Empty() {
			delete(sq.values, blockIndex)
			sq.size--

			if sq.size == 0 {
				// reset sequence mapping, in case we have no more values left
				// we do this in order to prevent a memory leak,
				// in case there are any orphanned mappings left,
				// due to aggressive evictions
				sq.sequences = make(map[uint64]int64)
			}
		}
	}

	return
}

// Empty implements sequenceCache.Empty
func (sq *inMemorySequenceCache) Empty() bool {
	sq.mux.RLock()
	defer sq.mux.RUnlock()

	return sq.size == 0
}

// newDataHistory creates a new data history
func newDataHistory() *dataHistory {
	return &dataHistory{
		min:       math.MaxUint64,
		max:       math.MaxUint64,
		latest:    math.MaxUint64,
		sequences: make(map[uint64]uint64),
		values:    make(map[uint64][]byte),
	}
}

// dataHistory is used to store multiple versions of the same blockIndex
// stored together with the sequence of each block
type dataHistory struct {
	latest    uint64
	min, max  uint64
	sequences map[uint64]uint64
	values    map[uint64][]byte
}

// Add a block version to the history,
// NOTE that the sequenceIndex has to be greater then the last given index
func (dh *dataHistory) Add(sequenceIndex uint64, data []byte) error {
	if sequenceIndex > dh.latest || dh.Empty() {
		dh.max++
		dh.latest = sequenceIndex
		dh.sequences[sequenceIndex] = dh.max
		dh.values[dh.max] = data
		return nil
	}

	return errOutdated
}

// Latest returns the last added data version,
// returns false in case there is no history yet
func (dh *dataHistory) Latest() ([]byte, bool) {
	value, ok := dh.values[dh.max]
	return value, ok
}

// LatestSequence returns the last added sequence
func (dh *dataHistory) LatestSequence() (uint64, bool) {
	if dh.latest != math.MaxUint64 {
		return dh.latest, true
	}
	return 0, false
}

// Trim the history until (and including) the given sequence,
// returning the data at the given index,
// and returning false in case that sequenceIndex,
// could not be found in this history
func (dh *dataHistory) Trim(sequenceIndex uint64) ([]byte, bool) {
	if index, ok := dh.sequences[sequenceIndex]; ok {
		// clean up sequence
		delete(dh.sequences, sequenceIndex)

		if dh.min == math.MaxUint64 {
			dh.min = 0
		}

		if index < dh.min {
			// index is too low,
			// might be that the data was already deleted
			return nil, false
		}

		value := dh.values[index]

		// clean up all outdated values
		for i := dh.min; i <= index; i++ {
			delete(dh.values, i)
		}

		// update state
		if index < dh.max {
			dh.min = index + 1
		} else { // index == dh.max
			// reset state, as we're out of values
			dh.sequences = make(map[uint64]uint64)
			dh.min = math.MaxUint64
			dh.max = math.MaxUint64
			dh.latest = math.MaxUint64
		}

		return value, true
	}

	return nil, false
}

// Empty returns true if there is no history yet
func (dh *dataHistory) Empty() bool {
	return dh.max == math.MaxUint64
}

var (
	// returned when a sequence is added,
	// which is older, then the data which is already cached
	errOutdated = errors.New("given sequence is outdated")

	// returned when applying action on an invalidated tlog storage
	errInvalidTlogStorage = errors.New("tlog storage is invalid")

	// returned when flush timed out
	errFlushTimeout = errors.New("flush timeout")
)

var (
	// TODO: come up with a better value, perhaps this value could be returned during
	//       the handshake phase from the server
	transactionChCapacity = 50

	toFlushChCapacity = 10000 // TODO : do we really need it to be so big?
)
