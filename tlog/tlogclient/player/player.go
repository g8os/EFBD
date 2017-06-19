package player

import (
	"context"
	"fmt"
	"strings"

	zerodiskcfg "github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/gonbdserver/nbd"
	"github.com/zero-os/0-Disk/nbdserver/ardb"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
)

// Player defines a tlog replay player.
// It could be used to restore the data based on transactions
// sent to tlog server
type Player struct {
	vdiskID string
	dec     *decoder.Decoder
	backend nbd.Backend
	ctx     context.Context
}

type OnReplayCb func(seq uint64) error

func OnReplayCbNone(seq uint64) error {
	return nil
}

// NewPlayer creates new tlog player
func NewPlayer(ctx context.Context, configPath string, serverConfigs []zerodiskcfg.StorageServerConfig,
	vdiskID, privKey, hexNonce string, k, m int) (*Player, error) {
	// create tlog redis pool
	pool, err := tlog.AnyRedisPool(tlog.RedisPoolConfig{
		VdiskID:                 vdiskID,
		RequiredDataServerCount: k + m,
		ConfigPath:              configPath,
		ServerConfigs:           serverConfigs,
		AutoFill:                true,
		AllowInMemory:           false,
	})

	if err != nil {
		return nil, err
	}

	// create ardb backend
	redisPool := ardb.NewRedisPool(nil)

	hotreloader, err := zerodiskcfg.NopHotReloader(configPath, zerodiskcfg.NBDServer)
	if err != nil {
		return nil, err
	}

	config := ardb.BackendFactoryConfig{
		Pool:              redisPool,
		ConfigHotReloader: hotreloader,
		LBACacheLimit:     ardb.DefaultLBACacheLimit,
	}
	fact, err := ardb.NewBackendFactory(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create factory:%v", err)
	}

	ec := &nbd.ExportConfig{
		Name:        vdiskID,
		Description: "zero-os/zerodisk",
		Driver:      "ardb",
		ReadOnly:    false,
		TLSOnly:     false,
	}

	backend, err := fact.NewBackend(ctx, ec)
	if err != nil {
		return nil, err
	}

	return NewPlayerWithPoolAndBackend(ctx, pool, backend, vdiskID, privKey, hexNonce, k, m)
}

// NewPlayerWithPoolAndBackend create new tlog player
// with given redis pool and nbd backend
func NewPlayerWithPoolAndBackend(ctx context.Context, pool tlog.RedisPool, backend nbd.Backend,
	vdiskID, privKey, hexNonce string, k, m int) (*Player, error) {

	dec, err := decoder.New(pool, k, m, vdiskID, privKey, hexNonce)
	if err != nil {
		return nil, err
	}

	return &Player{
		dec:     dec,
		backend: backend,
		ctx:     ctx,
		vdiskID: vdiskID,
	}, nil

}

// Replay replays the tlog by decoding data from a tlog RedisPool.
// The replay start from `startTs` timestamp.
func (p *Player) Replay(lmt decoder.Limiter) (uint64, error) {
	return p.ReplayWithCallback(lmt, OnReplayCbNone)
}

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
	return lastSeq, p.backend.Flush(p.ctx)
}

func (p *Player) ReplayAggregation(agg *schema.TlogAggregation, lmt decoder.Limiter) (uint64, error) {
	return p.ReplayAggregationWithCallback(agg, lmt, OnReplayCbNone)
}

// ReplayAggregation replays an aggregation
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

		seq = block.Sequence()

		offset := block.Offset()

		switch block.Operation() {
		case schema.OpWrite:
			data, err := block.Data()
			if err != nil {
				return seq - 1, fmt.Errorf("failed to get data block of offset=%v, err=%v", offset, err)
			}
			if _, err := p.backend.WriteAt(p.ctx, data, int64(offset)); err != nil {
				return seq - 1, fmt.Errorf("failed to WriteAt offset=%v, err=%v", offset, err)
			}
		case schema.OpWriteZeroesAt:
			if _, err := p.backend.WriteZeroesAt(p.ctx, int64(offset), int64(block.Size())); err != nil {
				return seq - 1, fmt.Errorf("failed to WriteAt offset=%v, err=%v", offset, err)
			}
		}
		// we flush it per sequence instead of per aggregation to
		// make sure we have sequence level accuracy about what we've replayed
		if err := p.backend.Flush(p.ctx); err != nil {
			return seq - 1, err
		}

		// execute the callback
		if err := onReplayCb(seq); err != nil {
			return seq, err
		}
	}
	return seq, nil
}
