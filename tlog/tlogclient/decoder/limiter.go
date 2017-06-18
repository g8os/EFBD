package decoder

import (
	"github.com/zero-os/0-Disk/tlog/schema"
)

// Limiter is interface that needs to be implemented
// in order for the decoder to know which aggregations
// it needs to decode
type Limiter interface {
	// EndAgg returns true if it is the end  of the
	// aggregation we want to decode
	EndAgg(*schema.TlogAggregation, schema.TlogBlock_List) bool

	// StartAgg returns true if it is the start of the
	// aggregation we want to decode
	StartAgg(*schema.TlogAggregation, schema.TlogBlock_List) bool

	StartBlock(schema.TlogBlock) bool
	EndBlock(schema.TlogBlock) bool
}

// LimitByTimestamp implements limiter based on the timestamp
type LimitByTimestamp struct {
	startTs uint64
	endTs   uint64
}

// NewLimitByTimestamp creates new LimitByTimestamp limiter
func NewLimitByTimestamp(startTs, endTs uint64) LimitByTimestamp {
	return LimitByTimestamp{
		startTs: startTs,
		endTs:   endTs,
	}
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

type LimitBySequence struct {
	startSeq uint64
	endSeq   uint64
}

func NewLimitBySequence(startSeq, endSeq uint64) LimitBySequence {
	return LimitBySequence{
		startSeq: startSeq,
		endSeq:   endSeq,
	}
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
