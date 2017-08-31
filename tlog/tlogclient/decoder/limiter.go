package decoder

import (
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
)

// Limiter is interface that needs to be implemented
// in order for the decoder to know which aggregations
// it needs to decode
type Limiter interface {

	// StartAgg returns true if it is the start of the
	// aggregation we want to decode
	StartAgg(*schema.TlogAggregation, schema.TlogBlock_List) bool

	// EndAgg returns true if it is the end  of the
	// aggregation we want to decode
	EndAgg(*schema.TlogAggregation, schema.TlogBlock_List) bool

	// StartBlock returns true if it is the start of block we want to decode
	StartBlock(schema.TlogBlock) bool

	// EndBlock returns true if it is the end of block we want to decode
	EndBlock(schema.TlogBlock) bool

	FromEpoch() int64

	ToEpoch() int64
}

// LimitByTimestamp implements limiter based on the timestamp
type LimitByTimestamp struct {
	startTs int64
	endTs   int64
}

// NewLimitByTimestamp creates new LimitByTimestamp limiter
func NewLimitByTimestamp(startTs, endTs int64) LimitByTimestamp {
	if endTs == 0 {
		endTs = tlog.TimeNowTimestamp()
	}
	return LimitByTimestamp{
		startTs: startTs,
		endTs:   endTs,
	}
}

func (lbt LimitByTimestamp) FromEpoch() int64 {
	return lbt.startTs
}

func (lbt LimitByTimestamp) ToEpoch() int64 {
	return lbt.endTs
}

// EndAgg implements Limiter.EndAgg
func (lbt LimitByTimestamp) EndAgg(agg *schema.TlogAggregation, blocks schema.TlogBlock_List) bool {
	if lbt.endTs == 0 {
		return false
	}

	// max timestamp of this aggregation is bigger than
	// endTs
	return blocks.At(blocks.Len()-1).Timestamp() > lbt.endTs && blocks.At(0).Timestamp() <= lbt.endTs
}

// StartAgg implements Limiter.StartAgg
func (lbt LimitByTimestamp) StartAgg(agg *schema.TlogAggregation, blocks schema.TlogBlock_List) bool {
	if lbt.startTs == 0 {
		return false
	}
	return blocks.At(blocks.Len()-1).Timestamp() >= lbt.startTs && blocks.At(0).Timestamp() < lbt.startTs
}

// EndBlock implementes Limiter.EndBlock
func (lbt LimitByTimestamp) EndBlock(block schema.TlogBlock) bool {
	if lbt.endTs == 0 {
		return false
	}
	return block.Timestamp() > lbt.endTs
}

// StartBlock implements Limiter.StartBlock
func (lbt LimitByTimestamp) StartBlock(block schema.TlogBlock) bool {
	if lbt.startTs == 0 {
		return true
	}
	return block.Timestamp() >= lbt.startTs
}

// LimitBySequence implement Limiter interface which is limited by
// start and end sequence
type LimitBySequence struct {
	startSeq uint64
	endSeq   uint64
	endTs    int64
}

// NewLimitBySequence creates new LimitBySequence object
func NewLimitBySequence(startSeq, endSeq uint64) LimitBySequence {
	return LimitBySequence{
		startSeq: startSeq,
		endSeq:   endSeq,
		endTs:    tlog.TimeNowTimestamp(),
	}
}

func (lbs LimitBySequence) FromEpoch() int64 {
	return 0
}

func (lbs LimitBySequence) ToEpoch() int64 {
	return lbs.endTs
}

// EndAgg implements Limiter.EndAgg
func (lbt LimitBySequence) EndAgg(agg *schema.TlogAggregation, blocks schema.TlogBlock_List) bool {
	if lbt.endSeq == 0 {
		return false
	}

	return blocks.At(blocks.Len()-1).Sequence() > lbt.endSeq && blocks.At(0).Sequence() <= lbt.endSeq
}

// StartAgg implements Limiter.StartAgg
func (lbt LimitBySequence) StartAgg(agg *schema.TlogAggregation, blocks schema.TlogBlock_List) bool {
	if lbt.startSeq == 0 {
		return false
	}
	return blocks.At(blocks.Len()-1).Sequence() >= lbt.startSeq && blocks.At(0).Sequence() < lbt.startSeq
}

// EndBlock implementes Limiter.EndBlock
func (lbt LimitBySequence) EndBlock(block schema.TlogBlock) bool {
	if lbt.endSeq == 0 {
		return false
	}
	return block.Sequence() > lbt.endSeq
}

// StartBlock implements Limiter.StartBlock
func (lbt LimitBySequence) StartBlock(block schema.TlogBlock) bool {
	if lbt.startSeq == 0 {
		return true
	}
	return block.Sequence() >= lbt.startSeq
}
