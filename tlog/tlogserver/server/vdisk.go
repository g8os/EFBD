package server

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/emirpasic/gods/sets/treeset"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
)

const (
	respChanSize = 10

	// tlogblock buffer size = flusher.flushSize * tlogBlockFactorSize
	// With buffer size that bigger than flushSize:
	// - we don't always block when flushing
	// - our RAM won't exploded because we still have upper limit
	tlogBlockFactorSize = 5
)

const (
	vdiskCmdForceFlushBlocking           = iota // blocking force flush
	vdiskCmdForceFlushAtSeq                     // non-blocking force flush with sequence param
	vdiskCmdClearUnorderedBlocksBlocking        // blocking clear all unordered blocks
	vdiskCmdWaitSlaveSync                       // wait for slave sync to be finished
)

// command for vdisk flusher
type vdiskFlusherCmd struct {
	cmdType  int8
	sequence uint64
	respCh   chan error
}

type vdiskCleanupFunc func(vdiskID string)

type vdisk struct {
	id       string
	respChan chan *BlockResponse // channel of responses to be sent to client

	configSource config.Source

	// channels for block receiver
	blockInputChan       chan *schema.TlogBlock // input channel of block received from client
	blockRecvCmdChan     chan uint8             // channel of block receiver command
	blockRecvCmdRespChan chan struct{}          // channel of block receiver command response

	// channels for flusher
	orderedBlockChan   chan *schema.TlogBlock // ordered blocks from blockInputChan
	flusherCmdChan     chan vdiskFlusherCmd   // channel of flusher command
	flusherCmdRespChan chan struct{}          // channel of flusher command response
	unwantedBlockChan  chan *schema.TlogBlock // channel of unwanted block : e.g.: double sent

	expectedSequence     uint64 // expected sequence to be received
	expectedSequenceLock sync.Mutex

	flusherConf *flusherConfig

	// connected clients table
	clientsTab     map[string]*net.TCPConn
	clientsTabLock sync.Mutex

	aggComm         *aggmq.AggComm
	aggMq           *aggmq.MQ
	apc             aggmq.AggProcessorConfig
	withSlaveSyncer bool
	ssMux           sync.Mutex

	storClient *stor.Client
}

// ID returns the ID of this vdisk
func (vd *vdisk) ID() string {
	return vd.id
}

// ResponseChan returns the channel to which Block Responses get sent
func (vd *vdisk) ResponseChan() <-chan *BlockResponse {
	return vd.respChan
}

// creates vdisk with given vdiskID, flusher, and first sequence.
// firstSequence is the very first sequence that this vdisk will receive.
// blocks with sequence < firstSequence are going to be ignored.
func newVdisk(parentCtx context.Context, vdiskID string, aggMq *aggmq.MQ, configSource config.Source,
	firstSequence uint64, flusherConf *flusherConfig, cleanup vdiskCleanupFunc) (*vdisk, error) {

	var aggComm *aggmq.AggComm

	// create slave syncer
	apc := aggmq.AggProcessorConfig{
		VdiskID:      vdiskID,
		DataShards:   flusherConf.DataShards,
		ParityShards: flusherConf.ParityShards,
		PrivKey:      flusherConf.PrivKey,
	}

	maxTlbInBuffer := flusherConf.FlushSize * tlogBlockFactorSize

	vd := &vdisk{
		id:           vdiskID,
		respChan:     make(chan *BlockResponse, respChanSize),
		configSource: configSource,

		// block receiver
		blockInputChan:       make(chan *schema.TlogBlock, maxTlbInBuffer),
		blockRecvCmdChan:     make(chan uint8, 1),
		blockRecvCmdRespChan: make(chan struct{}, 1),
		expectedSequence:     firstSequence,

		// flusher
		orderedBlockChan:   make(chan *schema.TlogBlock, maxTlbInBuffer),
		flusherCmdChan:     make(chan vdiskFlusherCmd, 1),
		flusherCmdRespChan: make(chan struct{}, 1),
		unwantedBlockChan:  make(chan *schema.TlogBlock, 2),
		flusherConf:        flusherConf,

		clientsTab:      make(map[string]*net.TCPConn),
		withSlaveSyncer: false,
		aggComm:         aggComm,
		aggMq:           aggMq,
		apc:             apc,
	}

	ctx, cancelFunc := context.WithCancel(parentCtx)

	if err := vd.watchConfig(ctx); err != nil {
		return nil, err
	}
	if err := vd.createFlusher(); err != nil {
		return nil, err
	}
	if err := vd.manageSlaveSync(); err != nil {
		return nil, err
	}

	// run vdisk goroutines
	go vd.runFlusher(ctx, cancelFunc)
	go vd.runBlockReceiver(ctx)
	go vd.handleSighup(ctx)
	go vd.cleanup(ctx, cleanup)

	return vd, nil
}

func (vd *vdisk) watchConfig(ctx context.Context) error {
	// get the cluster ID
	vtcCh, err := config.WatchVdiskTlogConfig(ctx, vd.configSource, vd.id)
	if err != nil {
		return err
	}
	vtc := <-vtcCh
	zeroStorClusterID := vtc.ZeroStorClusterID

	zeroStorClusterConfCh, err := config.WatchZeroStorClusterConfig(ctx, vd.configSource, zeroStorClusterID)
	if err != nil {
		return err
	}

	// wait until we have the config for the flusher
	<-zeroStorClusterConfCh

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
				//case _, err := <-config.WatchTlogStorageConfig(ctx, vd.configSource, vd.id):
				// TODO listen to this to watch for slave storage cluster
				// enable it while fixing https://github.com/zero-os/0-Disk/issues/401
			case vtc := <-vtcCh:
				if err != nil {
					continue
				}
				zeroStorClusterID = vtc.ZeroStorClusterID
			case <-zeroStorClusterConfCh:
				if err != nil {
					continue
				}
				vd.createFlusher()
			}
		}
	}()
	return nil
}

func (vd *vdisk) createFlusher() error {
	storConf, err := stor.ConfigFromConfigSource(vd.configSource, vd.ID(), vd.flusherConf.PrivKey,
		vd.flusherConf.DataShards, vd.flusherConf.ParityShards)
	if err != nil {
		return err
	}

	storClient, err := stor.NewClient(storConf)
	if err != nil {
		return err
	}
	vd.storClient = storClient
	return nil
}

// do all necessary cleanup for this vdisk
func (vd *vdisk) cleanup(ctx context.Context, cleanup vdiskCleanupFunc) {
	defer cleanup(vd.id)
	select {
	case <-ctx.Done():
		if err := vd.storClient.Close(); err != nil {
			log.Errorf("vdisk `%v` failed to close 0-stor client: %v", vd.id, err)
		}
		return
	}
}

// reset first sequence, what we need to do:
// - make sure we only have single connections for this vdisk
// - ignore all unordered blocks (blocking)
// - flush all unflushed ordered blocks (blocking)
// - reset it
func (vd *vdisk) resetFirstSequence(newSeq uint64, conn *net.TCPConn) error {
	log.Infof("vdisk %v reset first sequence to %v", vd.id, newSeq)
	vd.expectedSequenceLock.Lock()
	defer vd.expectedSequenceLock.Unlock()

	if newSeq == vd.expectedSequence { // we already in same page, do nothing
		return nil
	}
	if vd.numConnectedClient() != 1 {
		vd.disconnectExcept(conn)
	}

	// send ignore all unordered blocks command
	vd.blockRecvCmdChan <- vdiskCmdClearUnorderedBlocksBlocking
	<-vd.blockRecvCmdRespChan

	// send flush command
	vd.flusherCmdChan <- vdiskFlusherCmd{
		cmdType: vdiskCmdForceFlushBlocking,
	}
	<-vd.flusherCmdRespChan

	vd.expectedSequence = newSeq
	return nil
}

// force flush when vdisk receive the given sequence
func (vd *vdisk) forceFlushAtSeq(seq uint64) {
	vd.flusherCmdChan <- vdiskFlusherCmd{
		cmdType:  vdiskCmdForceFlushAtSeq,
		sequence: seq,
	}
}

// send wait slave sync command to the flusher
// we do it in blocking way
func (vd *vdisk) waitSlaveSync() error {
	// make sure it has syncer
	if !vd.withSlaveSyncer {
		log.Error("waitSlaveSync command received on vdisk with no slave syncer")
		return nil
	}
	log.Debugf("waitSlaveSync for vdisk: %v", vd.id)

	// send the command
	cmd := vdiskFlusherCmd{
		cmdType: vdiskCmdWaitSlaveSync,
		respCh:  make(chan error),
	}
	vd.flusherCmdChan <- cmd

	// wait and return the response
	err := <-cmd.respCh
	if err == nil {
		// we've successfully synced the slave
		// it means the slave is going to be used by nbdserver as it's master
		// so we disable it and kill the slave syncer
		vd.destroySlaveSync()
	}
	log.Debugf("waitSlaveSync for vdisk %v finished with err:%v", vd.id, err)

	return err
}

func (vd *vdisk) doWaitSlaveSync(respCh chan error, lastSeqFlushed uint64) {
	if !vd.withSlaveSyncer {
		respCh <- nil
	}
	respCh <- vd.aggComm.SendCmd(aggmq.CmdWaitSlaveSync, lastSeqFlushed)
}

// this is the flusher routine that receive the blocks and order it.
func (vd *vdisk) runBlockReceiver(ctx context.Context) {
	buffer := treeset.NewWith(tlogBlockComparator)

	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-vd.blockRecvCmdChan:
			if cmd == vdiskCmdClearUnorderedBlocksBlocking {
				buffer.Clear()
				vd.blockRecvCmdRespChan <- struct{}{}
			} else {
				log.Errorf("invalid block receiver command = %v", cmd)
			}
		case tlb := <-vd.blockInputChan:
			func() {
				// We create closure here to make the locking easy.
				// We are not creating separate func because the vd.expectedSequence
				// should be local to `runBlockReceiver` func
				vd.expectedSequenceLock.Lock()
				defer vd.expectedSequenceLock.Unlock()

				curSeq := tlb.Sequence()

				if curSeq == vd.expectedSequence {
					// it is the next sequence we wait
					vd.orderedBlockChan <- tlb
					vd.expectedSequence++
				} else if curSeq > vd.expectedSequence {
					// if this is not what we wait, buffer it
					buffer.Add(tlb)
				} else { // tlb.Sequence() < vd.expectedSequence -> duplicated message
					vd.unwantedBlockChan <- tlb
					return
				}

				if buffer.Empty() {
					return
				}

				// lets see the buffer again, check if we have blocks that
				// can be sent to the flusher.
				it := buffer.Iterator()
				blocks := []*schema.TlogBlock{}

				expected := vd.expectedSequence
				for it.Next() {
					block := it.Value().(*schema.TlogBlock)
					if block.Sequence() != expected {
						break
					}
					expected++
					blocks = append(blocks, block)
				}

				for _, block := range blocks {
					buffer.Remove(block) // we can only do it here because the iterator is read only
					vd.orderedBlockChan <- block
					vd.expectedSequence++
				}
			}()
		}
	}
}

// this is the flusher routine that does the flush asynchronously
func (vd *vdisk) runFlusher(ctx context.Context, cancelFunc context.CancelFunc) {
	defer cancelFunc()

	// buffer of all ordered tlog blocks
	tlogs := []*schema.TlogBlock{}

	// max sequence received by this flusher
	var maxSeq uint64

	// last sequence flushed by this flusher
	var lastSeqFlushed uint64

	// periodic flush interval
	pfDur := time.Duration(vd.flusherConf.FlushTime) * time.Second

	// periodic flush timer
	pfTimer := time.NewTimer(pfDur)

	var toFlushLen int
	var flusherCmd vdiskFlusherCmd
	var cmdType int8

	var seqToForceFlush uint64 // sequence to be force flushed
	var needForceFlushSeq bool // true if we wait for a sequence to be force flushed

	for {
		cmdType = -1
		select {
		case <-ctx.Done():
			return
		case tlb := <-vd.orderedBlockChan:
			// we receive tlog block, it already ordered
			tlogs = append(tlogs, tlb)

			maxSeq = tlb.Sequence()

			// check if we need to flush
			if needForceFlushSeq && tlb.Sequence() >= seqToForceFlush {
				// reset the flag and flush right now
				needForceFlushSeq = false
				toFlushLen = len(tlogs)
			} else if len(tlogs) < vd.flusherConf.FlushSize {
				// only flush if it > f.flushSize
				continue
			} else {
				toFlushLen = vd.flusherConf.FlushSize
			}

			pfTimer.Stop()
			pfTimer.Reset(pfDur)

		case <-pfTimer.C:
			// flush by timeout timer
			pfTimer.Reset(pfDur)
			if len(tlogs) == 0 {
				continue
			}
			toFlushLen = len(tlogs)

		case tlb := <-vd.unwantedBlockChan:
			// it is block that not expected to come:
			// - already received
			// - already flushed
			seq := tlb.Sequence()
			if seq <= lastSeqFlushed {
				vd.respChan <- &BlockResponse{
					Status:    int8(tlog.BlockStatusFlushOK),
					Sequences: []uint64{seq},
				}
			} // block received response already sent by main server handler
			continue

		case flusherCmd = <-vd.flusherCmdChan:
			// got command
			cmdType = flusherCmd.cmdType

			switch cmdType {
			case vdiskCmdForceFlushAtSeq: // force flush at sequence
				seqToForceFlush = flusherCmd.sequence
				if maxSeq < seqToForceFlush { // we don't have it yet
					needForceFlushSeq = true
					continue
				}
				// we already have the wanted sequence
				// flush right now if possible
				needForceFlushSeq = false

			case vdiskCmdForceFlushBlocking: // force flush right now, in blocking way

			case vdiskCmdWaitSlaveSync: // wait for slave sync
				vd.doWaitSlaveSync(flusherCmd.respCh, lastSeqFlushed)
			default:
				log.Errorf("invalid command to runFlusher: %v", flusherCmd)
				continue
			}

			if len(tlogs) == 0 {
				if cmdType == vdiskCmdForceFlushBlocking {
					// if it is blocking cmd, something else is waiting, notify him!
					vd.flusherCmdRespChan <- struct{}{}
				}
				continue
			}

			pfTimer.Stop()
			pfTimer.Reset(pfDur)

			toFlushLen = len(tlogs)
		}

		// get the blocks
		blocks := tlogs[:toFlushLen]
		tlogs = tlogs[toFlushLen:]

		status := tlog.BlockStatusFlushOK

		rawAgg, err := vd.storClient.ProcessStore(blocks)
		if err != nil {
			log.Infof("flush %v failed: %v", vd.id, err)
			status = tlog.BlockStatusFlushFailed
		}

		if cmdType == vdiskCmdForceFlushBlocking {
			// if it is blocking cmd, something else is waiting, notify him!
			vd.flusherCmdRespChan <- struct{}{}
		}

		seqs := make([]uint64, len(blocks))
		for i := 0; i < len(blocks); i++ {
			seqs[i] = blocks[i].Sequence()
		}

		// send response
		vd.respChan <- &BlockResponse{
			Status:    status.Int8(),
			Sequences: seqs,
		}

		if status != tlog.BlockStatusFlushOK {
			// we are failing to flush
			// it is better to kill ourself
			return
		}
		// update last sequence flushed
		if len(seqs) > 0 {
			lastSeqFlushed = seqs[len(seqs)-1]
		}
		// send aggregation to slave syncer
		vd.sendAggToSlaveSync(rawAgg)
	}
}

// number of connected clients to this vdisk
func (vd *vdisk) numConnectedClient() int {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()
	return len(vd.clientsTab)
}

// add client to the table of connected clients
func (vd *vdisk) addClient(conn *net.TCPConn) {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()

	vd.clientsTab[conn.RemoteAddr().String()] = conn
}

// remove client from the table of connected clients
func (vd *vdisk) removeClient(conn *net.TCPConn) {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()

	addr := conn.RemoteAddr().String()
	if _, ok := vd.clientsTab[addr]; !ok {
		log.Errorf("vdisk failed to remove client:%v", addr)
	} else {
		delete(vd.clientsTab, addr)
	}
}

// disconnect all connected clients except the
// given exceptConn connection
func (vd *vdisk) disconnectExcept(exceptConn *net.TCPConn) {
	vd.clientsTabLock.Lock()
	defer vd.clientsTabLock.Unlock()

	var conns []*net.TCPConn
	for _, conn := range vd.clientsTab {
		if conn != exceptConn {
			conns = append(conns, conn)
			conn.Close()
		}
	}
	for _, conn := range conns {
		delete(vd.clientsTab, conn.RemoteAddr().String())
	}
}

// the comparator function needed by https://godoc.org/github.com/emirpasic/gods/sets/treeset#NewWith
func tlogBlockComparator(a, b interface{}) int {
	tlbA := a.(*schema.TlogBlock)
	tlbB := b.(*schema.TlogBlock)

	seqA, seqB := tlbA.Sequence(), tlbB.Sequence()

	switch {
	case seqA < seqB:
		return -1
	case seqA > seqB:
		return 1
	default: // tlbA.Sequence() == tlbB.Sequence():
		return 0
	}
}

// handle SIGHUP
func (vd *vdisk) handleSighup(ctx context.Context) {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGHUP)

	for {
		select {
		case <-ctx.Done():
			return
		case <-sigs:
			vd.manageSlaveSync()
		}
	}
}

// send raw aggregation to slave syncer
func (vd *vdisk) sendAggToSlaveSync(rawAgg []byte) {
	vd.ssMux.Lock()
	defer vd.ssMux.Unlock()

	if vd.withSlaveSyncer {
		vd.aggComm.SendAgg(aggmq.AggMqMsg(rawAgg))
	}
}

// destroy slave syncer
func (vd *vdisk) destroySlaveSync() {
	vd.ssMux.Lock()
	defer vd.ssMux.Unlock()

	if !vd.withSlaveSyncer {
		return
	}
	vd.withSlaveSyncer = false
	vd.aggComm.Destroy()
	vd.aggComm = nil
}

// manage slave syncer life cycle
func (vd *vdisk) manageSlaveSync() error {
	vd.ssMux.Lock()
	defer vd.ssMux.Unlock()

	// it means the slave syncer doesn't activated globally
	if vd.aggMq == nil {
		return nil
	}

	// read config
	conf, err := config.ReadVdiskTlogConfig(vd.configSource, vd.id)
	if err != nil {
		return err
	}

	// we have no slave to sync, simply return
	if conf.SlaveStorageClusterID == "" {
		log.Infof("No slave for vdisk ID %v", vd.id)
		// kill the slave syncer first
		if vd.withSlaveSyncer {
			vd.aggComm.Destroy()
			vd.withSlaveSyncer = false
		}
		return nil
	}

	if vd.withSlaveSyncer {
		log.Infof("Restart slave syncer for vdisk: %v", vd.id)
		// slave syncer already exist, simply restart it
		vd.aggComm.SendCmd(aggmq.CmdRestartSlaveSyncer, 0)
		return nil
	}

	log.Infof("Activate slave syncer for vdisk: %v", vd.id)
	// activate slave syncer
	aggComm, err := vd.aggMq.AskProcessor(vd.apc)
	if err != nil {
		return err
	}

	vd.aggComm = aggComm
	vd.withSlaveSyncer = true
	return nil
}
