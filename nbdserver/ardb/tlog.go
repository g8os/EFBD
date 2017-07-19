package ardb

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient"
)

const (
	tlogFlushWaitFirstRetry  = 5 * time.Second  // retry(1st) force flush after this timeout
	tlogFlushWaitSecondRetry = 10 * time.Second // retry(2nd) force flush after this timeout
	tlogFlushWait            = 15 * time.Second // maximum duration we wait for flush to finish
)

// newTlogStorage creates a tlog storage backendStorage,
// wrapping around a given backend storage,
// piggy backing on the newTlogStorageWithClient function for the actual logic.
func newTlogStorage(vdiskID, tlogrpc, configPath string, blockSize int64, storage backendStorage) (backendStorage, error) {
	client, err := tlogclient.New(strings.Split(tlogrpc, ","), vdiskID, 0, true)
	if err != nil {
		return nil, fmt.Errorf("tlogStorage requires a valid tlogclient: %s", err.Error())
	}

	return newTlogStorageWithClient(vdiskID, configPath, blockSize, client, storage)
}

// newTlogStorage creates a tlog storage backendStorage,
// wrapping around a given backend storage,
// using the given tlog client to send its write transactions to the tlog server.
func newTlogStorageWithClient(vdiskID, configPath string, blockSize int64, client tlogClient,
	storage backendStorage) (backendStorage, error) {
	if storage == nil {
		return nil, errors.New("tlogStorage requires a non-nil backendStorage")
	}
	if client == nil {
		return nil, errors.New("tlogStorage requires a non-nil tlogClient")
	}

	return &tlogStorage{
		vdiskID:        vdiskID,
		tlog:           client,
		storage:        storage,
		cache:          newInMemorySequenceCache(),
		blockSize:      blockSize,
		sequence:       0,
		transactionCh:  make(chan *transaction, transactionChCapacity),
		toFlushCh:      make(chan []uint64, toFlushChCapacity),
		cacheEmptyCond: sync.NewCond(&sync.Mutex{}),
		configPath:     configPath,
	}, nil
}

// tlogStorage is a backendStorage implementation,
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
	storage       backendStorage
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

	configPath string
}

type transaction struct {
	Operation uint8
	Sequence  uint64
	Content   []byte
	Offset    uint64
	Timestamp uint64
	Size      uint64
}

// Set implements backendStorage.Set
func (tls *tlogStorage) Set(blockIndex int64, content []byte) error {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	return tls.set(blockIndex, content)
}

func (tls *tlogStorage) set(blockIndex int64, content []byte) error {
	var op uint8
	var length uint64

	// define tlog opcode, content and length
	if content == nil {
		op = schema.OpWriteZeroesAt
		length = uint64(tls.blockSize)
	} else if tls.isZeroContent(content) {
		op = schema.OpWriteZeroesAt
		length = uint64(len(content))
		content = nil
	} else {
		op = schema.OpWrite
		length = uint64(len(content))
	}

	// get next tlog sequence index,
	// never interact directly with tls.sequence
	// as this is not thread safe,
	// while any backendStorage HAS to be threadsafe
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
		Offset:    uint64(blockIndex * tls.blockSize),
		Timestamp: uint64(time.Now().UnixNano()),
		Content:   transactionContent,
		Size:      length,
	}

	// success!
	// content is being processed by the tlog server (hopefully)
	// and it's waiting in the history cache, until the tlog server is done
	return nil
}

// Merge implements backendStorage.Merge
func (tls *tlogStorage) Merge(blockIndex, offset int64, content []byte) error {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	mergedContent, err := tls.merge(blockIndex, offset, content)
	if err != nil {
		return err
	}

	// store new content
	return tls.set(blockIndex, mergedContent)
}

func (tls *tlogStorage) merge(blockIndex, offset int64, content []byte) ([]byte, error) {
	mergedContent, err := tls.get(blockIndex)
	if err != nil {
		return nil, err
	}

	// ensure merge block is of the proper size,
	// as to ensure a proper merge
	if len(mergedContent) == 0 {
		mergedContent = make([]byte, tls.blockSize)
	} else if int64(len(mergedContent)) < tls.blockSize {
		mc := make([]byte, tls.blockSize)
		copy(mc, mergedContent)
		mergedContent = mc
	}

	// merge new with old content
	copy(mergedContent[offset:], content)

	return mergedContent, nil
}

// Get implements backendStorage.Get
func (tls *tlogStorage) Get(blockIndex int64) (content []byte, err error) {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	content, err = tls.get(blockIndex)
	if err == nil {
		return
	}

	// there is issue with master, switch to ardb slave if possible
	if errSwitch := tls.switchToArdbSlave(); errSwitch != nil {
		log.Errorf("Failed to switchToArdbSlave: %v", errSwitch)
		return
	}

	return tls.get(blockIndex)
}

// Delete implements backendStorage.Delete
func (tls *tlogStorage) Delete(blockIndex int64) (err error) {
	tls.mux.Lock()
	defer tls.mux.Unlock()

	err = tls.set(blockIndex, nil)
	return
}

// Flush implements backendStorage.Flush
func (tls *tlogStorage) Flush() (err error) {
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
	for !finished {
		select {
		case <-time.After(tlogFlushWait):
			// if timeout, signal the condition variable anyway
			// to avoid the goroutine blocked forever
			tls.cacheEmptyCond.Signal()
			return errFlushTimeout
		case <-time.After(tlogFlushWaitFirstRetry): // retry it
			tls.tlog.ForceFlushAtSeq(latestSeq)
		case <-time.After(tlogFlushWaitSecondRetry): // retry it
			tls.tlog.ForceFlushAtSeq(latestSeq)
		case <-doneCh:
			// wait returned
			finished = true
		}
	}

	// flush actual storage
	tls.storageMux.Lock()
	defer tls.storageMux.Unlock()

	err = tls.storage.Flush()
	return
}

// Close implements backendStorage.Close
func (tls *tlogStorage) Close() (err error) {
	tls.storageMux.Lock()
	defer tls.storageMux.Unlock()

	err = tls.storage.Close()
	if err != nil {
		log.Info("error while closing internal tlog's storage: ", err)
	}

	err = tls.tlog.Close()
	if err != nil {
		log.Info("error while closing internal tlog's client: ", err)
	}

	return
}

// GoBackground implements backendStorage.GoBackground
func (tls *tlogStorage) GoBackground(ctx context.Context) {
	defer func() {
		log.Infof("GoBackground exited for vdisk: %v", tls.vdiskID)
		err := tls.tlog.Close()
		if err != nil {
			log.Info("error while closing tlog client: ", err)
		}
	}()
	sigHup := make(chan os.Signal, 1)
	signal.Notify(sigHup, syscall.SIGHUP)

	recvCh := tls.tlog.Recv()

	// used for sending our transactions
	go tls.transactionSender(ctx)

	// used for flushing from sequence cache
	go tls.flusher(ctx)

	for {
		select {
		case <-sigHup:
			tls.handleSighup()

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

			default:
				panic(fmt.Errorf(
					"tlog server had fatal failure for vdisk %s: %s",
					tls.vdiskID, res.Resp.Status))
			}

		case <-ctx.Done():
			log.Debugf("background context for tlogStorage %s aborting", tls.vdiskID)
			return
		}
	}
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
				transaction.Offset,
				transaction.Timestamp,
				transaction.Content,
				transaction.Size,
			)
			if err != nil {
				if err == tlogclient.ErrClientClosed {
					return
				}
				panic(fmt.Errorf(
					"tlogStorage couldn't send block %d (seq: %d): %s",
					transaction.Offset/uint64(tls.blockSize),
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

// switch to ardb slave if possible
func (tls *tlogStorage) switchToArdbSlave() error {
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
}

// see if there is a change in the tlogrpc
func (tls *tlogStorage) handleSighup() {
	conf, err := config.ReadConfig(tls.configPath, config.NBDServer)
	if err != nil {
		log.Errorf("handleSighup failed to read config: %v", err)
		return
	}

	if conf.TlogRPC != "" {
		tls.tlog.ChangeServerAddrs(strings.Split(conf.TlogRPC, ","))
	}
}

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

	return nil
}

func (tls *tlogStorage) flushAContent(blockIndex int64, data []byte) error {
	flush := func() error {
		if data == nil {
			return tls.storage.Delete(blockIndex)
		}
		return tls.storage.Set(blockIndex, data)
	}
	err := flush()
	if err == nil {
		return nil
	}

	// there is issue with master, switch to ardb slave if possible
	if errSwitch := tls.switchToArdbSlave(); errSwitch != nil {
		log.Errorf("Failed to switchToArdbSlave: %v", errSwitch)
		return err
	}

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
	content, err = tls.storage.Get(blockIndex)
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
	Send(op uint8, seq, offset, timestamp uint64, data []byte, size uint64) error
	ForceFlushAtSeq(uint64) error
	WaitNbdSlaveSync() error
	ChangeServerAddrs([]string)
	Recv() <-chan *tlogclient.Result
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
