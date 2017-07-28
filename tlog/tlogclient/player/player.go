package player

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/zero-os/0-Disk"
	zerodiskcfg "github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
)

// Player defines a tlog replay player.
// It could be used to restore the data based on transactions
// sent to tlog server
type Player struct {
	vdiskID      string
	dec          *decoder.Decoder
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
func NewPlayer(ctx context.Context, configInfo zerodisk.ConfigInfo, serverConfigs []zerodiskcfg.StorageServerConfig,
	vdiskID, privKey, hexNonce string, k, m int) (*Player, error) {
	// create tlog redis pool
	pool, err := tlog.AnyRedisPool(tlog.RedisPoolConfig{
		VdiskID:                 vdiskID,
		RequiredDataServerCount: k + m,
		ConfigInfo:              configInfo,
		ServerConfigs:           serverConfigs,
		AutoFill:                true,
		AllowInMemory:           false,
	})
	if err != nil {
		return nil, err
	}

	// get config to create block storage
	baseCfg, nbdCfg, err := zerodisk.ReadNBDConfig(vdiskID, configInfo)
	if err != nil {
		pool.Close()
		return nil, err
	}

	// create static provider,
	// as the tlog player does not require hot reloading.
	ardbProvider, err := ardb.StaticProvider(*nbdCfg, nil)
	if err != nil {
		pool.Close()
		return nil, err
	}

	blockStorage, err := storage.NewBlockStorage(storage.BlockStorageConfig{
		VdiskID:         vdiskID,
		TemplateVdiskID: nbdCfg.TemplateVdiskID,
		VdiskType:       baseCfg.Type,
		VdiskSize:       int64(baseCfg.Size) * ardb.GibibyteAsBytes,
		BlockSize:       int64(baseCfg.BlockSize),
	}, ardbProvider)
	if err != nil {
		pool.Close()
		ardbProvider.Close()
		return nil, err
	}

	return NewPlayerWithPoolAndStorage(ctx, pool, ardbProvider, blockStorage, vdiskID, privKey, hexNonce, k, m)
}

// NewPlayerWithPoolAndStorage create new tlog player
// with given redis pool and BlockStorage
func NewPlayerWithPoolAndStorage(ctx context.Context, pool tlog.RedisPool,
	connProvider ardb.ConnProvider, storage storage.BlockStorage,
	vdiskID, privKey, hexNonce string, k, m int) (*Player, error) {

	dec, err := decoder.New(pool, k, m, vdiskID, privKey, hexNonce)
	if err != nil {
		return nil, err
	}

	return &Player{
		dec:          dec,
		blockStorage: storage,
		connProvider: connProvider,
		ctx:          ctx,
		vdiskID:      vdiskID,
	}, nil

}

// Close releases all its resources
func (p *Player) Close() error {
	p.dec.Close()

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
// lmt implements the decoder.Limiter interface which specify start and end of the
// data we want to replay. It returns last sequence number it replayed.
func (p *Player) Replay(lmt decoder.Limiter) (uint64, error) {
	return p.ReplayWithCallback(lmt, onReplayCbNone)
}

// ReplayWithCallback replays
// lmt implements the decoder.Limiter interface which specify start and end of the
// It returns last sequence number it replayed.
func (p *Player) ReplayWithCallback(lmt decoder.Limiter, onReplayCb OnReplayCb) (uint64, error) {
	var lastSeq uint64
	var err error

	aggChan := p.dec.Decode(lmt)
	for {
		da, more := <-aggChan
		if !more {
			break
		}

		if da.Err != nil {
			return lastSeq, da.Err
		}

		if lastSeq, err = p.ReplayAggregationWithCallback(da.Agg, lmt, onReplayCb); err != nil {
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
