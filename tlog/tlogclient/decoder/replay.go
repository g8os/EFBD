package decoder

import (
	"context"
	"fmt"
	"strings"

	"github.com/zero-os/0-Disk/gonbdserver/nbd"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
)

// Replay replays the tlog by decoding data from a tlog RedisPool.
// The replay start from `startTs` timestamp.
func (d *Decoder) Replay(ctx context.Context, backend nbd.Backend, pool tlog.RedisPool, startTs uint64) error {
	aggChan := d.Decode(startTs)
	for {
		da, more := <-aggChan
		if !more {
			break
		}

		if da.Err != nil {
			return fmt.Errorf("failed to get aggregation: %v", da.Err)
		}

		if err := d.ReplayAggregation(ctx, da.Agg, backend); err != nil {
			return err
		}
	}
	return backend.Flush(ctx)
}

// ReplayAggregation replays an aggregation
func (d *Decoder) ReplayAggregation(ctx context.Context, agg *schema.TlogAggregation, backend nbd.Backend) error {
	// some small checking
	storedViskID, err := agg.VdiskID()
	if err != nil {
		return fmt.Errorf("failed to get vdisk id from aggregation: %v", err)
	}
	if strings.Compare(storedViskID, d.vdiskID) != 0 {
		return fmt.Errorf("vdisk id not mactched .expected=%v, got=%v", d.vdiskID, storedViskID)
	}

	// replay all the blocks
	blocks, err := agg.Blocks()
	for i := 0; i < blocks.Len(); i++ {
		block := blocks.At(i)
		offset := block.Offset()

		switch block.Operation() {
		case schema.OpWrite:
			data, err := block.Data()
			if err != nil {
				return fmt.Errorf("failed to get data block of offset=%v, err=%v", offset, err)
			}
			if _, err := backend.WriteAt(ctx, data, int64(offset)); err != nil {
				return fmt.Errorf("failed to WriteAt offset=%v, err=%v", offset, err)
			}
		case schema.OpWriteZeroesAt:
			if _, err := backend.WriteZeroesAt(ctx, int64(offset), int64(block.Size())); err != nil {
				return fmt.Errorf("failed to WriteAt offset=%v, err=%v", offset, err)
			}
		}
	}
	return backend.Flush(ctx)
}
