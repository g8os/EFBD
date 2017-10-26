package aggmq

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrSlaveSyncTimeout returned when slave syncer doesn't give response
	// until some amount of time
	ErrSlaveSyncTimeout = errors.New("slave sync timed out")
)

// AggProcessorConfig defines config given to the aggregation
// processor
type AggProcessorConfig struct {
	VdiskID string
	PrivKey string
}

// AggProcessorReq defines request to the processor provider
type AggProcessorReq struct {
	Comm   *AggComm
	Config AggProcessorConfig
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
func (mq *MQ) AskProcessor(apc AggProcessorConfig) (*AggComm, error) {
	mq.mux.Lock()
	defer mq.mux.Unlock()

	// check if the processor already existed
	if comm, ok := mq.Comms[apc.VdiskID]; ok {
		return comm, nil
	}

	comm := newAggComm(mq, apc.VdiskID)
	// ask for processor
	mq.NeedProcessorCh <- AggProcessorReq{
		Comm:   comm,
		Config: apc,
	}

	// wait for the resp
	select {
	case err := <-mq.NeedProcessorResp:
		if err != nil {
			return nil, fmt.Errorf("slave syncer failed to be created:%v", err)
		}
	case <-time.After(30 * time.Second):
		return nil, ErrSlaveSyncTimeout
	}

	mq.Comms[apc.VdiskID] = comm
	return comm, nil
}

func (mq *MQ) deleteProcessor(vdiskID string) {
	mq.mux.Lock()
	defer mq.mux.Unlock()

	delete(mq.Comms, vdiskID)
}
