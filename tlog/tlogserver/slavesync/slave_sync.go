package slavesync

import (
	"bytes"
	"context"
	"sync"

	"zombiezen.com/go/capnproto2"

	zerodiskcfg "github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
)

// Manager defines slave syncer manager
type Manager struct {
	apMq       *aggmq.MQ
	syncers    map[string]*slaveSyncer
	configPath string
	mux        sync.Mutex
}

// NewManager creates new slave syncer manager
func NewManager(apMq *aggmq.MQ, configPath string) *Manager {
	m := &Manager{
		apMq:       apMq,
		configPath: configPath,
		syncers:    make(map[string]*slaveSyncer),
	}
	return m
}

func (m *Manager) Run() {
	for {
		apr := <-m.apMq.NeedProcessorCh
		m.handleReq(apr)
	}
}

func (m *Manager) handleReq(apr aggmq.AggProcessorReq) {
	m.mux.Lock()
	defer m.mux.Unlock()

	log.Debugf("slavesync manager : create for vdisk: %v", apr.Config.VdiskID)

	// check if the syncer already exist
	if _, ok := m.syncers[apr.Config.VdiskID]; ok {
		m.apMq.NeedProcessorResp <- nil
		return
	}

	// create slave syncer
	ss, err := newSlaveSyncer(apr.Context, m.configPath, apr.Config, apr.Comm, m)
	if err != nil {
		log.Errorf("failed to create slave syncer: %v", err)
		m.apMq.NeedProcessorResp <- err
		return
	}
	m.syncers[apr.Config.VdiskID] = ss

	// send response
	m.apMq.NeedProcessorResp <- nil

	log.Debugf("slave syncer created for vdisk: %v", apr.Config.VdiskID)
}
func (m *Manager) remove(vdiskID string) {
	m.mux.Lock()
	defer m.mux.Unlock()

	delete(m.syncers, vdiskID)
}

type slaveSyncer struct {
	ctx     context.Context
	aggComm *aggmq.AggComm
	player  *player.Player
	mgr     *Manager
	vdiskID string
}

func newSlaveSyncer(ctx context.Context, configPath string, apc aggmq.AggProcessorConfig,
	aggComm *aggmq.AggComm, mgr *Manager) (*slaveSyncer, error) {

	serverConfigs, err := zerodiskcfg.ParseCSStorageServerConfigStrings("")
	if err != nil {
		return nil, err
	}

	player, err := player.NewPlayer(ctx, configPath, serverConfigs, apc.VdiskID, apc.PrivKey,
		apc.HexNonce, apc.K, apc.M)
	if err != nil {
		return nil, err
	}

	ss := &slaveSyncer{
		vdiskID: apc.VdiskID,
		ctx:     ctx,
		aggComm: aggComm,
		mgr:     mgr,
		player:  player,
	}
	go ss.run()

	return ss, nil
}

func (ss *slaveSyncer) run() {
	log.Infof("run slave syncer for vdisk: %v", ss.vdiskID)
	for {
		select {
		case <-ss.ctx.Done():
			ss.mgr.remove(ss.vdiskID)
			log.Infof("slave syncer for vdisk: %v stopped", ss.vdiskID)
			return
		case rawAgg := <-ss.aggComm.RecvAgg():
			msg, err := capnp.NewDecoder(bytes.NewReader([]byte(rawAgg))).Decode()
			if err != nil {
				log.Errorf("failed to decode agg:%v", err)
			}
			agg, err := schema.ReadRootTlogAggregation(msg)
			if err != nil {
				log.Errorf("failed to read root tlog:%v", err)
			}

			if err := ss.player.ReplayAggregation(&agg, 0, 0); err != nil {
				log.Errorf("replay agg failed")
			}
		}
	}
}
