package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zero-os/0-stor/client/lib"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/flusher"
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
	// non-blocking force flush with sequence param
	vdiskCmdForceFlushAtSeq = iota

	// wait for slave syncer to finish syncing to
	// slave storage cluster
	vdiskCmdWaitSlaveSync

	// ignore all sequences before
	// the sequence provided in the command
	vdiskCmdIgnoreSeqBefore
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

	// channels for flusher
	orderedBlockChan   chan *schema.TlogBlock // ordered blocks from blockInputChan
	flusherCmdChan     chan vdiskFlusherCmd   // channel of flusher command
	flusherCmdRespChan chan struct{}          // channel of flusher command response

	expectedSequence uint64 // expected sequence to be received
	mux              sync.Mutex

	flusherConf *flusherConfig

	// connected clients table
	clientConn     *net.TCPConn
	clientConnLock sync.Mutex

	aggComm         *aggmq.AggComm
	aggMq           *aggmq.MQ
	apc             aggmq.AggProcessorConfig
	withSlaveSyncer bool
	ssMux           sync.Mutex

	ctx        context.Context
	cancelFunc context.CancelFunc

	// true if this vdisk ready to be used
	// - have no remote tlog server to coordinate
	// - remote tlog server to coordinate/wait already dies
	ready            bool
	coordConnectAddr string // remote tlog server to coordinate before marking ourself as ready

	storClient *stor.Client
	flusher    *flusher.Flusher
}

// ID returns the ID of this vdisk
func (vd *vdisk) ID() string {
	return vd.id
}

// ResponseChan returns the channel to which Block Responses get sent
func (vd *vdisk) ResponseChan() <-chan *BlockResponse {
	return vd.respChan
}

// creates vdisk with given vdiskID
func newVdisk(parentCtx context.Context, vdiskID string, aggMq *aggmq.MQ, configSource config.Source,
	flusherConf *flusherConfig, cleanup vdiskCleanupFunc, coordConnectAddr string) (*vdisk, error) {

	var aggComm *aggmq.AggComm

	ctx, cancelFunc := context.WithCancel(parentCtx)
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

		// flusher
		orderedBlockChan:   make(chan *schema.TlogBlock, maxTlbInBuffer),
		flusherCmdChan:     make(chan vdiskFlusherCmd, 1),
		flusherCmdRespChan: make(chan struct{}, 1),
		flusherConf:        flusherConf,

		withSlaveSyncer: false,
		aggComm:         aggComm,
		aggMq:           aggMq,
		apc:             apc,

		ctx:              ctx,
		cancelFunc:       cancelFunc,
		coordConnectAddr: coordConnectAddr,
	}

	if vd.coordConnectAddr == "" {
		vd.ready = true
	}

	if err := vd.watchConfig(); err != nil {
		cancelFunc()
		return nil, err
	}
	if err := vd.createFlusher(); err != nil {
		cancelFunc()
		return nil, fmt.Errorf("failed to create flusher: %v", err)
	}
	if err := vd.manageSlaveSync(); err != nil {
		cancelFunc()
		return nil, err
	}

	// run vdisk goroutines
	go vd.runFlusher()
	go vd.handleSighup()
	go vd.cleanup(cleanup)

	log.Infof("vdisk %v created", vd.id)
	return vd, nil
}

func (vd *vdisk) Ready() bool {
	vd.mux.Lock()
	defer vd.mux.Unlock()
	return vd.ready
}

func (vd *vdisk) watchConfig() error {
	// get the cluster ID
	vtcCh, err := config.WatchVdiskTlogConfig(vd.ctx, vd.configSource, vd.id)
	if err != nil {
		return err
	}
	vtc := <-vtcCh
	zeroStorClusterID := vtc.ZeroStorClusterID

	zeroStorClusterConfCh, err := config.WatchZeroStorClusterConfig(vd.ctx, vd.configSource, zeroStorClusterID)
	if err != nil {
		return err
	}

	// wait until we have the config for the flusher
	<-zeroStorClusterConfCh

	go func() {
		for {
			select {
			case <-vd.ctx.Done():
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
	// creates stor client
	storClient, err := stor.NewClientFromConfigSource(vd.configSource, vd.id, vd.flusherConf.PrivKey,
		vd.flusherConf.DataShards, vd.flusherConf.ParityShards)
	if err != nil {
		return err
	}
	vd.storClient = storClient

	vdiskConf, err := config.ReadVdiskStaticConfig(vd.configSource, vd.id)
	if err != nil {
		return err
	}

	// creates flusher
	vd.flusher = flusher.NewWithStorClient(storClient, vd.flusherConf.FlushSize, int(vdiskConf.BlockSize))
	return nil
}

// do all necessary cleanup for this vdisk
func (vd *vdisk) cleanup(cleanup vdiskCleanupFunc) {
	defer func() {
		log.Infof("vdisk %v cleanup", vd.id)
		cleanup(vd.id)
	}()
	select {
	case <-vd.ctx.Done():
		if err := vd.storClient.Close(); err != nil {
			log.Errorf("vdisk `%v` failed to close 0-stor client: %v", vd.id, err)
		}
		return
	}
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
		return
	}
	respCh <- vd.aggComm.SendCmd(aggmq.CmdWaitSlaveSync, lastSeqFlushed)
}

// connects the given connection to this vdisk
func (vd *vdisk) connect(conn *net.TCPConn) (uint64, error) {
	if err := vd.attachConn(conn); err != nil {
		return 0, err
	}

	if !vd.Ready() {
		return 0, nil
	}

	lastSeq, err := vd.loadLastFlushedSequence()
	if err != nil {
		vd.removeConn(conn)
	}

	return lastSeq, err
}

// load last flushed sequence from the stor
func (vd *vdisk) loadLastFlushedSequence() (uint64, error) {
	lastSeq, err := vd.storClient.LoadLastSequence()

	vd.mux.Lock()
	defer vd.mux.Unlock()

	if err == stor.ErrNoFlushedBlock {
		vd.expectedSequence = tlog.FirstSequence
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	expectedSequence := lastSeq + 1

	// ask flusher to ignore all sequences before
	// the last flushed sequence
	cmd := vdiskFlusherCmd{
		cmdType:  vdiskCmdIgnoreSeqBefore,
		sequence: expectedSequence,
		respCh:   make(chan error),
	}
	vd.flusherCmdChan <- cmd
	<-cmd.respCh

	vd.expectedSequence = expectedSequence

	return lastSeq, nil
}

// this is the flusher routine that does the flush asynchronously
func (vd *vdisk) runFlusher() {
	defer vd.cancelFunc()

	var (
		// max sequence received by this flusher
		maxSeq uint64

		// last sequence flushed by this flusher
		lastSeqFlushed uint64

		// periodic flush interval
		pfDur = time.Duration(vd.flusherConf.FlushTime) * time.Second

		// periodic flush timer
		pfTimer = time.NewTimer(pfDur)

		flusherCmd vdiskFlusherCmd
		cmdType    int8

		// sequence to be force flushed
		seqToForceFlush uint64

		// true if we wait for a sequence to be force flushed
		needForceFlushSeq bool
	)

	for {
		cmdType = -1
		select {
		case <-vd.ctx.Done():
			return
		case tlb := <-vd.orderedBlockChan:
			// we receive tlog block, it already ordered
			if err := vd.flusher.AddBlock(tlb); err != nil {
				log.Errorf("vdisk `%v` failed to add block: %v", vd.id, err)
				return
			}

			maxSeq = tlb.Sequence()

			// check if we need to flush
			if needForceFlushSeq && tlb.Sequence() >= seqToForceFlush {
				// reset the flag and flush right now
				needForceFlushSeq = false
			} else if !vd.flusher.Full() {
				// only flush if full
				continue
			}

			pfTimer.Stop()
			pfTimer.Reset(pfDur)

		case <-pfTimer.C:
			// flush by timeout timer
			pfTimer.Reset(pfDur)
			if vd.flusher.Empty() {
				continue
			}

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

			case vdiskCmdIgnoreSeqBefore:
				if err := vd.flusher.IgnoreSeqBefore(flusherCmd.sequence); err != nil {
					log.Errorf("vdisk `%v` failed to ignore sequence before `%v`: %v",
						vd.id, flusherCmd.sequence, err)
					return
				}
				flusherCmd.respCh <- nil
				continue

			case vdiskCmdWaitSlaveSync: // wait for slave sync
				vd.doWaitSlaveSync(flusherCmd.respCh, lastSeqFlushed)
			default:
				log.Errorf("invalid command to runFlusher: %v", flusherCmd)
				continue
			}

			if vd.flusher.Empty() {
				continue
			}

			pfTimer.Stop()
			pfTimer.Reset(pfDur)
		}

		// get the blocks
		status := tlog.BlockStatusFlushOK

		// flush to 0-stor
		rawAgg, seqs, err := vd.flusher.Flush()
		if err != nil {
			log.Errorf("flush %v failed: %v", vd.id, err)
			notifyFlushError(err)
			status = tlog.BlockStatusFlushFailed
		}

		// send response
		vd.respChan <- &BlockResponse{
			Status:    status.Int8(),
			Sequences: seqs,
		}

		if status != tlog.BlockStatusFlushOK {
			// We are failing to flush!
			// Stop! So the client could connect to other server
			// Or reconnect to us again and then we retry the flush process
			log.Errorf("vdisk %v is failing to flush, stop it", vd.id)
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

func notifyFlushError(err error) {
	se, ok := err.(lib.ShardError)
	if !ok {
		return
	}
	for _, e := range se.Errors() {
		subj := func() log.MessageSubject {
			switch e.Kind {
			case lib.ShardTypeEtcd:
				return log.SubjectETCD
			default:
				return log.SubjectZeroStor
			}
		}()
		status := func() log.MessageStatus {
			switch e.Code {
			case lib.StatusTimeoutError:
				return log.StatusClusterTimeout
			case lib.StatusInvalidShardAddress:
				return log.StatusInvalidConfig
			default:
				return log.StatusUnknownError
			}
		}()
		log.Broadcast(status, subj, e.Addrs)
	}
}

// handle SIGHUP
func (vd *vdisk) handleSighup() {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGHUP)

	for {
		select {
		case <-vd.ctx.Done():
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
