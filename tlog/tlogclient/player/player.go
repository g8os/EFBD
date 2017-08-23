package player

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
)

// Player defines a tlog replay player.
// It could be used to restore the data based on transactions
// sent to tlog server
type Player struct {
	vdiskID      string
	storCli      *stor.Client
	blockStorage storage.BlockStorage
	connProvider ardb.ConnProvider
	ctx          context.Context
}

// OnReplayCb defines func signature which can be used as callback
// for the Replay* functions.
// This callback is going to be executed on each block replay.
type OnReplayCb func(seq uint64) error

func onReplayCbNone(seq uint64) error {
	return nil
}

// NewPlayer creates new tlog player
func NewPlayer(ctx context.Context, source config.Source,
	vdiskID, privKey string, k, m int) (*Player, error) {

	// get config to create block storage
	vdiskCfg, err := config.ReadVdiskStaticConfig(source, vdiskID)
	if err != nil {
		return nil, err
	}
	nbdCfg, err := config.ReadNBDStorageConfig(source, vdiskID, vdiskCfg)
	if err != nil {
		return nil, err
	}

	// create static provider,
	// as the tlog player does not require hot reloading.
	ardbProvider, err := ardb.StaticProvider(*nbdCfg, nil)
	if err != nil {
		return nil, err
	}

	blockStorage, err := storage.NewBlockStorage(storage.BlockStorageConfig{
		VdiskID:         vdiskID,
		TemplateVdiskID: vdiskCfg.TemplateVdiskID,
		VdiskType:       vdiskCfg.Type,
		BlockSize:       int64(vdiskCfg.BlockSize),
	}, ardbProvider)
	if err != nil {
		ardbProvider.Close()
		return nil, err
	}

	return NewPlayerWithStorage(ctx, source, ardbProvider, blockStorage, vdiskID, privKey, k, m)
}

// NewPlayerWithPoolAndStorage create new tlog player
// with given BlockStorage
func NewPlayerWithStorage(ctx context.Context, source config.Source,
	connProvider ardb.ConnProvider, storage storage.BlockStorage,
	vdiskID, privKey string, k, m int) (*Player, error) {

	storConf, err := stor.ConfigFromConfigSource(source, vdiskID, privKey, k, m)
	if err != nil {
		return nil, err
	}
	storCli, err := stor.NewClient(storConf)
	if err != nil {
		return nil, err
	}

	return &Player{
		blockStorage: storage,
		connProvider: connProvider,
		ctx:          ctx,
		vdiskID:      vdiskID,
		storCli:      storCli,
	}, nil

}

// Close releases all its resources
func (p *Player) Close() error {
	p.storCli.Close()

	// TODO:
	// choose a universal error combinator solution
	// as code like this is a mess

	if p.connProvider == nil {
		return p.blockStorage.Close()
	}

	errA := p.connProvider.Close()
	errB := p.blockStorage.Close()
	if errA != nil {
		if errB != nil {
			return errors.New(errA.Error() + "; " + errB.Error())
		}

		return errA
	}

	return errB
}

// Replay replays the tlog by decoding data from the tlog blockchains.
func (p *Player) Replay(lmt decoder.Limiter) (uint64, error) {
	return p.ReplayWithCallback(lmt, onReplayCbNone)
}

// ReplayWithCallback replays
// lmt implements the decoder.Limiter interface which specify start and end of the
// It returns last sequence number it replayed.
func (p *Player) ReplayWithCallback(lmt decoder.Limiter, onReplayCb OnReplayCb) (uint64, error) {
	var lastSeq uint64
	var err error

	wrCh := p.storCli.Walk(lmt.FromEpoch(), lmt.ToEpoch())
	for {
		wr, more := <-wrCh
		if !more {
			break
		}

		if wr.Err != nil {
			return lastSeq, wr.Err
		}

		if lastSeq, err = p.ReplayAggregationWithCallback(wr.Agg, lmt, onReplayCb); err != nil {
			return lastSeq, err
		}
	}
	return lastSeq, p.blockStorage.Flush()
}

// ReplayAggregation replays an aggregation.
// It returns last sequence number it replayed.
func (p *Player) ReplayAggregation(agg *schema.TlogAggregation, lmt decoder.Limiter) (uint64, error) {
	return p.ReplayAggregationWithCallback(agg, lmt, onReplayCbNone)
}

// ReplayAggregationWithCallback replays an aggregation with a callback.
// The callback is executed after it replay a block.
// It returns last sequence number it replayed.
func (p *Player) ReplayAggregationWithCallback(agg *schema.TlogAggregation, lmt decoder.Limiter,
	onReplayCb OnReplayCb) (uint64, error) {

	// some small checking
	storedViskID, err := agg.VdiskID()
	if err != nil {
		return 0, fmt.Errorf("failed to get vdisk id from aggregation: %v", err)
	}
	if strings.Compare(storedViskID, p.vdiskID) != 0 {
		return 0, fmt.Errorf("vdisk id not mactched .expected=%v, got=%v", p.vdiskID, storedViskID)
	}

	var seq uint64
	var index int64
	var data []byte

	// replay all the blocks
	blocks, err := agg.Blocks()
	for i := 0; i < blocks.Len(); i++ {
		block := blocks.At(i)

		if !lmt.StartBlock(block) {
			continue
		}

		if lmt.EndBlock(block) {
			return seq, nil
		}

		seq, index = block.Sequence(), block.Index()

		switch block.Operation() {
		case schema.OpSet:
			data, err = block.Data()
			if err != nil {
				return seq - 1, fmt.Errorf("failed to get data block %v, err=%v", index, err)
			}
			if err = p.blockStorage.SetBlock(index, data); err != nil {
				return seq - 1, fmt.Errorf("failed to set block %v, err=%v", index, err)
			}
		case schema.OpDelete:
			if err = p.blockStorage.DeleteBlock(index); err != nil {
				return seq - 1, fmt.Errorf("failed to delete block %v, err=%v", index, err)
			}
		}
		// we flush it per sequence instead of per aggregation to
		// make sure we have sequence level accuracy about what we've replayed
		if err = p.blockStorage.Flush(); err != nil {
			return seq - 1, err
		}

		// execute the callback
		if err = onReplayCb(seq); err != nil {
			return seq, err
		}
	}
	return seq, nil
}
