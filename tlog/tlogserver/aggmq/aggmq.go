package aggmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrProcessorNotCreated = errors.New("agg processor failed to be created")
)

// AggProcessorConfig defines config given to the aggregation
// processor
type AggProcessorConfig struct {
	VdiskID  string
	K        int
	M        int
	PrivKey  string
	HexNonce string
}

// AggProcessorReq defines request to the processor provider
type AggProcessorReq struct {
	Comm    *AggComm
	Config  AggProcessorConfig
	Context context.Context
}

// MQ defines this message queue
type MQ struct {
	NeedProcessorCh   chan AggProcessorReq
	NeedProcessorResp chan error
	Comms             map[string]*AggComm
	mux               sync.Mutex
}

// NewMQ creates new MQ
func NewMQ() *MQ {
	return &MQ{
		NeedProcessorCh:   make(chan AggProcessorReq),
		NeedProcessorResp: make(chan error),
		Comms:             make(map[string]*AggComm),
	}
}

// AskProcessor as for aggregation processor.
// we currently only have slave syncer
func (mq *MQ) AskProcessor(ctx context.Context, apc AggProcessorConfig) (*AggComm, error) {
	mq.mux.Lock()
	defer mq.mux.Unlock()

	// check if the processor already existed
	if comm, ok := mq.Comms[apc.VdiskID]; ok {
		return comm, nil
	}

	comm := newAggComm()
	// ask for processor
	mq.NeedProcessorCh <- AggProcessorReq{
		Comm:    comm,
		Config:  apc,
		Context: ctx,
	}

	// wait for the resp
	// TODO : add timeout
	err := <-mq.NeedProcessorResp
	if err != nil {
		return nil, fmt.Errorf("%v:%v", ErrProcessorNotCreated, err)
	}

	mq.Comms[apc.VdiskID] = comm
	return comm, nil
}
