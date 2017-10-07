package tlog

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/zero-os/0-Disk/nbd/ardb"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient"

	"github.com/garyburd/redigo/redis"
)

// Storage creates a tlog storage BlockStorage,
// wrapping around a given backend storage,
// using the given tlog client to send its write transactions to the tlog server.
func Storage(ctx context.Context, vdiskID, clusterID string, configSource config.Source, blockSize int64, storage storage.BlockStorage, mdProvider ardb.MetadataConnProvider, client tlogClient) (storage.BlockStorage, error) {
	if storage == nil {
		return nil, errors.New("tlogStorage requires a non-nil BlockStorage")
	}

	metadata, err := loadOrNewTlogMetadata(vdiskID, mdProvider)
	if err != nil {
		return nil, errors.New("tlogStorage requires a valid metadata ARDB provider: " + err.Error())
	}

	ctx, cancel := context.WithCancel(ctx)
	tlogClusterCh, err := config.WatchTlogClusterConfig(ctx, configSource, clusterID)
	if err != nil {
		cancel()
		return nil, errors.New("tlogStorage requires a valid config source: " + err.Error())
	}

	var tlogClusterConfig config.TlogClusterConfig
	select {
	case tlogClusterConfig = <-tlogClusterCh:
	case <-ctx.Done():
		cancel()
		return nil, errors.New("tlogStorage requires active context")
	}

	var serverReady bool
	if client == nil {
		client, serverReady, err = tlogclient.New(tlogClusterConfig.Servers, vdiskID)
		if err != nil {
			cancel()
			return nil, errors.New("tlogStorage requires valid tlogclient: " + err.Error())
		}

		if metadata.lastFlushedSequence < client.LastFlushedSequence() {
			// TODO call tlog player if last flushed sequence from tlog server is
			// higher than us.
			// see: https://github.com/zero-os/0-Disk/issues/230
			log.Infof("possile error when starting vdisk `%v`: might need to sync with tlog", vdiskID)
		}
	}

	tlogStorage := &tlogStorage{
		vdiskID:        vdiskID,
		tlog:           client,
		storage:        storage,
		metadata:       metadata,
		cache:          newInMemorySequenceCache(),
		blockSize:      blockSize,
		sequence:       client.LastFlushedSequence() + 1,
		transactionCh:  make(chan *transaction, transactionChCapacity),
		toFlushCh:      make(chan []uint64, toFlushChCapacity),
		cacheEmptyCond: sync.NewCond(&sync.Mutex{}),
		done:           make(chan struct{}, 1),
		tlogReadyCh:    make(chan uint64),
		tlogReady:      serverReady,
	}

	err = tlogStorage.spawnBackgroundGoroutine(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	go tlogStorage.goTlogRPCReloader(ctx, tlogClusterCh, cancel)
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
	mux           sync.RWMutex
	tlog          tlogClient
	storage       storage.BlockStorage
	metadata      *tlogMetadata
	cache         sequenceCache
	blockSize     int64
	sequence      uint64
	sequenceMux   sync.Mutex
	transactionCh chan *transaction
	toFlushCh     chan []uint64 // channel of sequences to be flushed

	// mutex for the ardb storage
	storageMux sync.Mutex

	// condition variable used to signal when the cache is empty
	cacheEmptyCond *sync.Cond

	done chan struct{}

	tlogReadyCh      chan uint64
	tlogReadyMux     sync.Mutex
	tlogReady        bool
	tlogNotReadyBuff []writeOp
}

type transaction struct {
	Operation uint8
	Sequence  uint64
	Content   []byte
	Index     int64
	Timestamp int64
}

type writeOp struct {
	blockIndex int64
	content    []byte
}

// SetBlock implements BlockStorage.SetBlock
func (tls *tlogStorage) SetBlock(blockIndex int64, content []byte) error {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	// insert to buffer if tlog not ready yet
	tls.tlogReadyMux.Lock()
	defer tls.tlogReadyMux.Unlock()
	if !tls.tlogReady {
		tls.tlogNotReadyBuff = append(tls.tlogNotReadyBuff, writeOp{
			blockIndex: blockIndex,
			content:    content,
		})
		return nil
	}

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

	// insert to buffer if tlog not ready yet
	tls.tlogReadyMux.Lock()
	defer tls.tlogReadyMux.Unlock()
	if !tls.tlogReady {
		tls.tlogNotReadyBuff = append(tls.tlogNotReadyBuff, writeOp{
			blockIndex: blockIndex,
			content:    nil,
		})
		return nil
	}

	err = tls.set(blockIndex, nil)
	return
}

// Flush implements BlockStorage.Flush
func (tls *tlogStorage) Flush() error {
	// wait for tlog to be ready
	// TODO : find a way without waiting
	for {
		tls.tlogReadyMux.Lock()
		if tls.tlogReady {
			tls.tlogReadyMux.Unlock()
			break
		}
		time.Sleep(1 * time.Second)
		tls.tlogReadyMux.Unlock()
	}

	tls.mux.Lock()
	defer tls.mux.Unlock()

	// ForceFlush at the latest sequence
	tls.tlog.ForceFlushAtSeq(tls.getLatestSequence())

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
		case <-time.After(FlushWaitRetry): // retry it
			tls.tlog.ForceFlushAtSeq(tls.getLatestSequence())

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
	err = tls.metadata.Flush()
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
	close(tls.done)

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

func (tls *tlogStorage) waitTlogServerReady() {
	defer log.Infof("tlogServer for %v is ready", tls.vdiskID)

	newLastSeq := <-tls.tlogReadyCh

	tls.tlogReadyMux.Lock()
	defer tls.tlogReadyMux.Unlock()

	// update last sequence
	tls.sequenceMux.Lock()
	tls.sequence = newLastSeq + 1
	tls.sequenceMux.Unlock()

	// resume all helds IO
	for _, op := range tls.tlogNotReadyBuff {
		tls.set(op.blockIndex, op.content)
	}

	// mark tlog server as ready
	tls.tlogNotReadyBuff = []writeOp{}
	tls.tlogReady = true
}
func (tls *tlogStorage) spawnBackgroundGoroutine(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	recvCh := tls.tlog.Recv()

	if !tls.tlogReady { // wait until our server is ready
		go func() {
			log.Infof("wait tlogserver for vdisk `%v` to be ready", tls.vdiskID)

			tls.waitTlogServerReady()
		}()
	}

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
					panic(fmt.Errorf(
						"tlog server resulted in error for vdisk %s: %s",
						tls.vdiskID, res.Err))
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
				case tlog.BlockStatusReady:
					tls.tlogReadyCh <- res.Resp.Sequences[0]
					log.Info("tlog connection is ready")
				default:
					panic(fmt.Errorf(
						"tlog server had fatal failure for vdisk %s: %s",
						tls.vdiskID, res.Resp.Status))
				}

			case <-tls.done:
				log.Info("tls.done tlogclient receiver should be finished by now")
				tls.done = nil
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
				if err == tlogclient.ErrClientClosed {
					return
				}
				panic(fmt.Errorf(
					"tlogStorage couldn't send block %d (seq: %d): %s",
					transaction.Index,
					transaction.Sequence,
					err,
				))
			}

		case <-ctx.Done():
			log.Debugf("transaction sender for tlogStorage %s aborting", tls.vdiskID)
			return
		}
	}
}

// flusher is spawned by the `GoBackground` method,
// and is meant to flush transaction from sequence cache to storage,
// over a seperate goroutine
func (tls *tlogStorage) flusher(ctx context.Context) {
	for {
		select {
		case seqs := <-tls.toFlushCh:
			err := tls.flushCachedContent(seqs)
			if err != nil {
				panic(fmt.Errorf(
					"failed to write cached content into storage for vdisk %s: %s",
					tls.vdiskID, err))
			}

		case <-ctx.Done():
			log.Debugf("flusher for tlogStorage %s aborting", tls.vdiskID)
			return
		}
	}

}

func (tls *tlogStorage) goTlogRPCReloader(ctx context.Context, ch <-chan config.TlogClusterConfig, cancel func()) {
	defer cancel()

	for {
		select {
		case cfg := <-ch:
			if len(cfg.Servers) == 0 {
				// TODO: notify 0-orchestrator
				log.Errorf(
					"nbd config for vdisk %s no longer specifies tlog rpcs", tls.vdiskID)
				continue
			}

			tls.tlog.ChangeServerAddresses(cfg.Servers)

		case <-ctx.Done():
			return
		}
	}
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
	tls.metadata.SetLastFlushedSequence(sequences[len(sequences)-1])
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

// loadOrNewTlogMetadata load metadata from storage or
// creates a new tlog metadata object.
// Defauling to nil-values of properties for all properties
// that weren't defined yet in the ARDB metadata storage.
func loadOrNewTlogMetadata(vdiskID string, provider ardb.MetadataConnProvider) (*tlogMetadata, error) {
	if provider == nil {
		// used for testing purposes only
		return new(tlogMetadata), nil
	}

	conn, err := provider.MetadataConnection()
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if provider.DisableMetadataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  vdiskID,
					},
				)
			}
		}
		return nil, err
	}
	defer conn.Close()

	key := MetadataKey(vdiskID)
	md, err := deserializeTlogMetadata(key, vdiskID, conn)
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if provider.DisableMetadataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  vdiskID,
					},
				)
			}
		}
		return nil, err
	}

	md.provider = provider
	return md, nil
}

func deserializeTlogMetadata(key, vdiskID string, metaConn redis.Conn) (*tlogMetadata, error) {
	lastFlushedSequence, err := redis.Uint64(
		metaConn.Do("HGET", key, tlogMetadataLastFlushedSequenceField))
	if err != nil && err != redis.ErrNil {
		return nil, err
	}

	return newTlogMetadata(lastFlushedSequence, vdiskID, key), nil
}

type tlogMetadata struct {
	lastFlushedSequence uint64

	vdiskID, key string
	provider     ardb.MetadataConnProvider
	mux          sync.Mutex
}

func newTlogMetadata(lastFlushedSequence uint64, vdiskID, key string) *tlogMetadata {
	return &tlogMetadata{
		lastFlushedSequence: lastFlushedSequence,
		vdiskID:             vdiskID,
		key:                 key,
	}
}

// SetLastFlushedSequence sets the given seq as the last flushed sequence,
// only if the given sequence is greater than the one already used.
func (md *tlogMetadata) SetLastFlushedSequence(seq uint64) bool {
	md.mux.Lock()
	defer md.mux.Unlock()

	if seq <= md.lastFlushedSequence {
		return false
	}

	md.lastFlushedSequence = seq
	return true
}

// Flush all the metadata for this tlogstorage into the ARDB metadata storage server.
func (md *tlogMetadata) Flush() error {
	if md.provider == nil {
		return nil // nothing to do
	}

	md.mux.Lock()
	defer md.mux.Unlock()

	conn, err := md.provider.MetadataConnection()
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", md.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if md.provider.DisableMetadataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  md.vdiskID,
					},
				)
			}
		}
		return err
	}
	defer conn.Close()

	err = md.serialize(conn)
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", md.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if md.provider.DisableMetadataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  md.vdiskID,
					},
				)
			}
		}
		return err
	}

	return nil
}

func (md *tlogMetadata) serialize(conn redis.Conn) error {
	_, err := conn.Do("HSET", md.key, tlogMetadataLastFlushedSequenceField, md.lastFlushedSequence)
	return err
}

// MetadataKey returns the key of the ARDB hashmap,
// which contains all the metadata stored for a tlog storage.
func MetadataKey(vdiskID string) string {
	return tlogMetadataKeyPrefix + vdiskID
}

// CreateMetadata creates all metadata of a tlog-enabled storage.
func CreateMetadata(vdiskID string, lastFlushedSequence uint64, cluster *config.StorageClusterConfig) error {
	metaCfg, err := cluster.FirstAvailableServer()
	if err != nil {
		return err
	}

	conn, err := ardb.GetConnection(*metaCfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	metadata := newTlogMetadata(lastFlushedSequence, vdiskID, MetadataKey(vdiskID))

	return metadata.serialize(conn)
}

// DeleteMetadata deletes all metadata of a tlog-enabled storage,
// from a given metadata server using the given vdiskID.
func DeleteMetadata(serverCfg config.StorageServerConfig, vdiskIDs ...string) error {
	// get connection to metadata storage server
	conn, err := ardb.GetConnection(serverCfg)
	if err != nil {
		return fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
	}
	defer conn.Close()

	for _, vdiskID := range vdiskIDs {
		// delete the actual metadata (if it existed)
		err = conn.Send("DEL", MetadataKey(vdiskID))
		if err != nil {
			return fmt.Errorf(
				"couldn't add %s to the delete tlog metadata batch: %v",
				vdiskID, err)
		}
	}

	err = conn.Flush()
	if err != nil {
		return fmt.Errorf(
			"couldn't flush the delete tlog metadata batch: %v", err)
	}

	var errors flushErrors
	for _, vdiskID := range vdiskIDs {
		_, err = conn.Receive()
		if err != nil {
			errors = append(errors, fmt.Errorf(
				"couldn't delete tlog metadata for %s: %v", vdiskID, err))
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// CopyMetadata copies all metadata of a tlog-enabled storage
// from a sourceID to a targetID, within the same cluster or between different clusters.
func CopyMetadata(sourceID, targetID string, sourceCluster, targetCluster *config.StorageClusterConfig) error {
	// validate source cluster
	if sourceCluster == nil {
		return errors.New("no source cluster given")
	}

	// define whether or not we're copying between different servers.
	if targetCluster == nil {
		targetCluster = sourceCluster
	}

	// get first available storage server

	metaSourceCfg, err := sourceCluster.FirstAvailableServer()
	if err != nil {
		return err
	}
	metaTargetCfg, err := targetCluster.FirstAvailableServer()
	if err != nil {
		return err
	}

	if metaSourceCfg.Equal(metaTargetCfg) {
		conn, err := ardb.GetConnection(*metaSourceCfg)
		if err != nil {
			return fmt.Errorf("couldn't connect to ardb: %s", err.Error())
		}
		defer conn.Close()

		return copyMetadataSameConnection(sourceID, targetID, conn)
	}

	conns, err := ardb.GetConnections(*metaSourceCfg, *metaTargetCfg)
	if err != nil {
		return fmt.Errorf("couldn't connect to ardb: %s", err.Error())
	}
	defer func() {
		conns[0].Close()
		conns[1].Close()
	}()
	return copyMetadataDifferentConnections(sourceID, targetID, conns[0], conns[1])
}

func copyMetadataDifferentConnections(sourceID, targetID string, connA, connB redis.Conn) error {
	sourceKey := MetadataKey(sourceID)
	metadata, err := deserializeTlogMetadata(sourceKey, sourceID, connA)
	if err != nil {
		return fmt.Errorf(
			"couldn't deserialize source tlog metadata for %s: %v", sourceID, err)
	}

	metadata.key = MetadataKey(targetID)
	err = metadata.serialize(connB)
	if err != nil {
		return fmt.Errorf(
			"couldn't serialize destination tlog metadata for %s: %v", targetID, err)
	}

	return nil
}

func copyMetadataSameConnection(sourceID, targetID string, conn redis.Conn) error {
	log.Infof("dumping tlog metadata of vdisk %q and restoring it as tlog metadata of vdisk %q",
		sourceID, targetID)

	sourceKey, targetKey := MetadataKey(sourceID), MetadataKey(targetID)
	_, err := copyMetadataSameConnScript.Do(conn, sourceKey, targetKey)
	return err
}

var copyMetadataSameConnScript = redis.NewScript(0, `
	local source = ARGV[1]
	local dest = ARGV[2]
	
	if redis.call("EXISTS", source) == 0 then
		return
	end
	
	if redis.call("EXISTS", dest) == 1 then
		redis.call("DEL", dest)
	end
	
	redis.call("RESTORE", dest, 0, redis.call("DUMP", source))
`)

const (
	tlogMetadataKeyPrefix                = "tlog:"
	tlogMetadataLastFlushedSequenceField = "lfseq"
)

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
