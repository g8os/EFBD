package player

import (
	"context"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
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
	closer       Closer
	ctx          context.Context
}

// OnReplayCb defines func signature which can be used as callback
// for the Replay* functions.
// This callback is going to be executed on each block replay.
type OnReplayCb func(seq uint64) error

// Closer defines an interface of an object that can be closed
type Closer interface {
	Close() error
}

// NewPlayer creates new tlog player
func NewPlayer(ctx context.Context, source config.Source, vdiskID, privKey string) (*Player, error) {
	ardbPool := ardb.NewPool(nil)
	blockStorage, err := storage.BlockStorageFromConfig(vdiskID, source, ardbPool)
	if err != nil {
		ardbPool.Close()
		return nil, err
	}

	return NewPlayerWithStorage(ctx, source, ardbPool, blockStorage, vdiskID, privKey)
}

// NewPlayerWithStorage create new tlog player
// with given BlockStorage
func NewPlayerWithStorage(ctx context.Context, source config.Source, closer Closer, storage storage.BlockStorage,
	vdiskID, privKey string) (*Player, error) {

	storConf, err := stor.ConfigFromConfigSource(source, vdiskID, privKey)
	if err != nil {
		return nil, err
	}
	storCli, err := stor.NewClient(storConf)
	if err != nil {
		return nil, err
	}

	if storage == nil {
		return nil, errors.New("Player requires non-nil BlockStorage")
	}

	return &Player{
		blockStorage: storage,
		closer:       closer,
		ctx:          ctx,
		vdiskID:      vdiskID,
		storCli:      storCli,
	}, nil

}

// Close releases all its resources
func (p *Player) Close() error {
	p.storCli.Close()
	if p.closer != nil {
		p.closer.Close()
	}

	return p.blockStorage.Close()
}

// Replay replays the tlog by decoding data from the tlog blockchains.
func (p *Player) Replay(lmt decoder.Limiter) (uint64, error) {
	return p.ReplayWithCallback(lmt, nil)
}

// ReplayWithCallback replays
// lmt implements the decoder.Limiter interface which specify start and end of the
// It returns last sequence number it replayed.
func (p *Player) ReplayWithCallback(lmt decoder.Limiter, onReplayCb OnReplayCb) (uint64, error) {
	var lastSeq uint64
	var err error

	for wr := range p.storCli.Walk(lmt.FromEpoch(), lmt.ToEpoch()) {
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
	n, err := p.ReplayAggregationWithCallback(agg, lmt, nil)
	if err != nil {
		return 0, err
	}

	return n, p.blockStorage.Flush()
}

// ReplayAggregationWithCallback replays an aggregation with a callback.
// The callback is executed after it replay a block.
// It returns last sequence number it replayed.
func (p *Player) ReplayAggregationWithCallback(agg *schema.TlogAggregation, lmt decoder.Limiter,
	onReplayCb OnReplayCb) (uint64, error) {

	var seq uint64
	var index int64
	var data []byte

	// replay all the blocks
	blocks, err := agg.Blocks()
	for i := 0; i < int(agg.Size()); i++ {
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
				return seq - 1, errors.Wrapf(err, "failed to get data block %v", index)
			}
			if err = p.blockStorage.SetBlock(index, data); err != nil {
				return seq - 1, errors.Wrapf(err, "failed to set block %v", index)
			}
		case schema.OpDelete:
			if err = p.blockStorage.DeleteBlock(index); err != nil {
				return seq - 1, errors.Wrapf(err, "failed to delete block %v", index)
			}
		}

		if onReplayCb == nil {
			continue
		}

		// we flush it per sequence instead of per aggregation to
		// make sure we have sequence level accuracy about what we've replayed
		// which might be needed by the callback
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
