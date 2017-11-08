package slavesync

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/zero-os/0-Disk/config"
	zerodiskerror "github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	"zombiezen.com/go/capnproto2"
)

const (
	_ = iota

	// cmdWaitSlaveSync is command given to slave syncer.
	// It indicates to the syncer that we want to wait for slave syncer to finish it's work
	cmdWaitSlaveSync

	// cmdRestartSlaveSyncer is command to restart the slave syncer
	cmdRestartSlaveSyncer

	// cmdKillMe is command to kill the slave syncer
	cmdKillMe
)

var (
	// ErrSlaveSyncTimeout returned in case of slave syncing
	// couldn't be done in the given duration
	ErrSlaveSyncTimeout = errors.New("slave sync timeout")
)

// command to this slave syncer
type command struct {
	Type int
	Seq  uint64
}

type syncResult struct {
	lastSeq uint64
	err     error
}

// slaveSyncer defines a tlog slave syncer
// which implementes SlaveSyncer interface
type slaveSyncer struct {
	ctx              context.Context
	vdiskID          string
	player           *player.Player // tlog replay player
	mgr              *Manager
	metaCli          *stor.MetaClient
	lastSeqSyncedKey []byte // key of the last sequence synced
	configSource     config.Source
	privKey          string

	// channel of the raw aggregations sent to this slave syncer
	aggCh chan []byte

	// channel of the command send to slave syncer
	cmdCh chan command

	// channel of last synced sequence
	// it only used while waitForSync = true
	lastSyncedCh chan syncResult

	// we put these three  variables here
	// to make it survive in case of restart
	waitForSync   bool   // true if we currently wait for sync event
	seqToWait     uint64 // sequence to wait to be synced
	lastSyncedSeq uint64 // last synced sequence
}

// newSlaveSyncer creates a new slave syncer
func newSlaveSyncer(ctx context.Context, configSource config.Source, vdiskID, privKey string, mgr *Manager) (*slaveSyncer, error) {

	ss := &slaveSyncer{
		ctx:              ctx,
		mgr:              mgr,
		configSource:     configSource,
		vdiskID:          vdiskID,
		privKey:          privKey,
		aggCh:            make(chan []byte, 1000),
		cmdCh:            make(chan command, 1),
		lastSyncedCh:     make(chan syncResult),
		lastSeqSyncedKey: []byte("tlog:last_slave_sync_seq:" + vdiskID),
	}
	return ss, ss.init()
}

func (ss *slaveSyncer) init() error {
	// Create BlockStorage, to store in the slave cluster
	vdiskConfig, err := config.ReadVdiskStaticConfig(ss.configSource, ss.vdiskID)
	if err != nil {
		return err
	}
	slaveCluster, err := NewSlaveCluster(ss.ctx, ss.vdiskID, ss.configSource)
	if err != nil {
		return err
	}
	blockStorage, err := storage.NewBlockStorage(storage.BlockStorageConfig{
		VdiskID:         ss.vdiskID,
		TemplateVdiskID: vdiskConfig.TemplateVdiskID,
		VdiskType:       vdiskConfig.Type,
		BlockSize:       int64(vdiskConfig.BlockSize),
		LBACacheLimit:   ardb.DefaultLBACacheLimit,
	}, slaveCluster, nil)
	if err != nil {
		slaveCluster.Close()
		return err
	}

	// tlog replay player
	player, err := player.NewPlayerWithStorage(
		ss.ctx, ss.configSource, slaveCluster, blockStorage,
		ss.vdiskID, ss.privKey)
	if err != nil {
		blockStorage.Close()
		slaveCluster.Close()
		return err
	}
	ss.player = player

	// create meta client
	storConf, err := stor.ConfigFromConfigSource(ss.configSource, ss.vdiskID, "")
	if err != nil {
		player.Close()
		return err
	}

	metaCli, err := stor.NewMetaClient(storConf.MetaShards)
	if err != nil {
		player.Close()
		return err
	}

	ss.metaCli = metaCli

	err = ss.start()
	if err != nil {
		player.Close()
		metaCli.Close()
		return err
	}

	return nil
}

func (ss *slaveSyncer) restart() {
	if err := ss.init(); err != nil {
		log.Errorf("restarting slave syncer for `%v` failed: %v", ss.vdiskID, err)
		return
	}
}

// start the slave syncer.
// make sure to sync the latest synced sequence with
// what stored in tlogserver
func (ss *slaveSyncer) start() error {
	var err error
	// get slavesyncer's latest synced sequence
	ss.lastSyncedSeq, err = ss.getLastSyncedSeq()
	if err != nil {
		return zerodiskerror.Wrap(err, "getLastSyncedSeq failed")
	}

	// get tlog's latest flushed sequence and catch up if needed
	// we can do it by simply replaying all sequence after last synced sequence
	ss.lastSyncedSeq, err = ss.player.ReplayWithCallback(ss.decodeLimiter(ss.lastSyncedSeq),
		ss.setLastSyncedSeq)
	if err != nil {
		return err
	}

	go ss.run()

	return nil
}

// Close closes this slave syncer
func (ss *slaveSyncer) Close() {
	ss.player.Close()
}

func (ss *slaveSyncer) Restart() {
	ss.cmdCh <- command{
		Type: cmdRestartSlaveSyncer,
	}
}

// SendAgg implements SlaveSyncer.SendAgg interface
func (ss *slaveSyncer) SendAgg(rawAgg []byte) {
	ss.aggCh <- rawAgg
}

// WaitSync implements SlaveSyncer.WaitSync interface
func (ss *slaveSyncer) WaitSync(seq uint64, timeout time.Duration) error {
	ss.cmdCh <- command{
		Type: cmdWaitSlaveSync,
		Seq:  seq,
	}

	ctx, cancelFunc := context.WithTimeout(ss.ctx, timeout)
	defer cancelFunc()

	// wait until the sequence synced
	// or timeout
	for {
		select {
		case <-ctx.Done():
			return ErrSlaveSyncTimeout
		case syncRes := <-ss.lastSyncedCh:
			if syncRes.err != nil {
				return syncRes.err
			}

			if syncRes.lastSeq >= seq {
				return nil
			}
		}
	}
}

// Stop implements SlaveSyncer.Stop interface
func (ss *slaveSyncer) Stop() {
	ss.cmdCh <- command{
		Type: cmdKillMe,
	}
}

// run this slave syncer
func (ss *slaveSyncer) run() {
	log.Infof("slave syncer (%v): started", ss.vdiskID)

	defer log.Infof("slave syncer (%v): exited", ss.vdiskID)
	// true if we want to restart this syncer
	// we simply restart in case of error, it is simpler yet more robust
	var needRestart bool

	// true if tlog vdisk already exited
	// we also need to exit :)
	var needToExit bool

	// closure to mark that we've synced the slave
	// we put it here so we don't need to worry about mutex
	finishWaitForSync := func(err error) {
		if !ss.waitForSync {
			return
		}

		// got error in sync
		// finish our waiting
		if err != nil {
			ss.waitForSync = false
			ss.lastSyncedCh <- syncResult{
				err: err,
			}
			return
		}

		// last synced is more than our waited sequence
		// finish our wait
		if ss.lastSyncedSeq >= ss.seqToWait {
			ss.waitForSync = false
			ss.lastSyncedCh <- syncResult{
				lastSeq: ss.lastSyncedSeq,
			}
		}
	}

	defer func() {
		if needRestart && !needToExit {
			ss.restart()
		} else {
			ss.mgr.remove(ss.vdiskID)
		}
	}()

	for {
		select {
		case <-ss.ctx.Done():
			// our producer (tlog's vdisk) exited
			// so we are!
			// but we need to wait until we sync all of it
			if len(ss.aggCh) == 0 {
				return
			}
			needToExit = true

		case rawAgg := <-ss.aggCh:
			// raw aggregation []byte
			seq, err := ss.replay(rawAgg, ss.lastSyncedSeq)
			if err != nil {
				log.Errorf("replay failed : %v", err)

				// TODO report to ays

				finishWaitForSync(err)

				// exit
				needToExit = true
				return
			}
			ss.lastSyncedSeq = seq

			finishWaitForSync(nil)

			if needToExit && len(ss.aggCh) == 0 {
				return
			}

		case cmd := <-ss.cmdCh:
			// receive a command
			switch cmd.Type {
			case cmdKillMe:
				log.Infof("slave syncer (%v): killed", ss.vdiskID)

				ss.Close()
				ss.mgr.remove(ss.vdiskID)

				return
			case cmdRestartSlaveSyncer:
				log.Infof("slave syncer (%v): restarted", ss.vdiskID)

				ss.Close()
				needRestart = true

				return
			case cmdWaitSlaveSync:
				// wait for slave to be fully synced
				ss.seqToWait = cmd.Seq
				ss.waitForSync = true

				finishWaitForSync(nil)
			}

		}
	}
}

func (ss *slaveSyncer) decodeLimiter(lastSeqSynced uint64) decoder.Limiter {
	startSeq := lastSeqSynced + 1
	if lastSeqSynced == 0 {
		startSeq = 0
	}
	return decoder.NewLimitBySequence(startSeq, 0)
}

// replay a raw aggregation (in []byte) to the ardb slave
func (ss *slaveSyncer) replay(rawAgg []byte, lastSeqSynced uint64) (uint64, error) {
	msg, err := capnp.NewDecoder(bytes.NewReader(rawAgg)).Decode()
	if err != nil {
		return 0, zerodiskerror.Wrap(err, "failed to decode agg")
	}

	agg, err := schema.ReadRootTlogAggregation(msg)
	if err != nil {
		return 0, zerodiskerror.Wrap(err, "failed to read root tlog")
	}

	return ss.player.ReplayAggregationWithCallback(&agg, ss.decodeLimiter(lastSeqSynced), ss.setLastSyncedSeq)
}
