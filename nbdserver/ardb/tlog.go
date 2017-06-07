package ardb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient"
	tlogserver "github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

const (
	// maximum duration we wait for flush to finish
	tlogFlushWait = 10 * time.Millisecond
)

func newTlogStorage(vdiskID, tlogrpc string, blockSize int64, storage backendStorage) (backendStorage, error) {
	client, err := tlogclient.New(tlogrpc, vdiskID, 0, true)
	if err != nil {
		return nil, fmt.Errorf("tlogStorage requires a valid tlogclient: %s", err.Error())
	}

	return newTlogStorageWithClient(vdiskID, blockSize, client, storage)
}

func newTlogStorageWithClient(vdiskID string, blockSize int64, client tlogClient, storage backendStorage) (backendStorage, error) {
	if storage == nil {
		return nil, errors.New("tlogStorage requires a non-nil storage")
	}

	return &tlogStorage{
		vdiskID:        vdiskID,
		tlog:           client,
		storage:        storage,
		cache:          newInMemorySequenceCache(),
		blockSize:      blockSize,
		sequence:       0,
		transactionCh:  make(chan *transaction, transactionChCapacity),
		cacheEmptyCond: sync.NewCond(&sync.Mutex{}),
	}, nil
}

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
	// condition variable used to signal when the cache is empty
	cacheEmptyCond *sync.Cond
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
		Timestamp: uint64(time.Now().Unix()),
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
	tls.mux.RLock()
	defer tls.mux.RUnlock()

	content, err = tls.get(blockIndex)
	return
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

	select {
	case <-time.After(tlogFlushWait):
		// if timeout, signal the condition variable anyway
		// to avoid the goroutine blocked forever
		tls.cacheEmptyCond.Signal()
	case <-doneCh:
		// wait returned
	}

	// make sure that the sequence cache is empty,
	// as that is the only proof we have that the tlog server
	// is done flushing all our transactions
	if !tls.cache.Empty() {
		err = fmt.Errorf(
			"sequence cache for tlog storage %s is not yet empty",
			tls.vdiskID)
		return
	}

	// flush actual storage
	err = tls.storage.Flush()
	return
}

// Close implements backendStorage.Close
func (tls *tlogStorage) Close() (err error) {
	err = tls.storage.Close()
	if err != nil {
		log.Info("error while closing internal tlog's storage: ", err)
	}

	return
}

// GoBackground implements backendStorage.GoBackground
func (tls *tlogStorage) GoBackground(ctx context.Context) {
	defer func() {
		err := tls.tlog.Close()
		if err != nil {
			log.Info("error while closing tlog client: ", err)
		}
	}()

	recvCh := tls.tlog.Recv()

	// used for sending our transactions
	go tls.transactionSender(ctx)

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
				go tls.flushCachedContent(res.Resp.Sequences)
				/*err := tls.flushCachedContent(res.Resp.Sequences)
				if err != nil {
					panic(fmt.Errorf(
						"failed to write cached content into storage for vdisk %s: %s",
						tls.vdiskID, err))
				}*/

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

	tls.mux.Lock()
	defer tls.mux.Unlock()

	elements := tls.cache.Evict(sequences...)

	var errs flushErrors
	for blockIndex, data := range elements {
		var err error

		if data == nil {
			err = tls.storage.Delete(blockIndex)
		} else {
			err = tls.storage.Set(blockIndex, data)
		}

		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs
	}

	return nil
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
)

var (
	// TODO: come up with a better value, perhaps this value could be returned during
	//       the handshake phase from the server
	transactionChCapacity = tlogserver.DefaultConfig().FlushSize * 2
)
