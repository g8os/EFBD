package aggmq

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrProcessorNotCreated = errors.New("agg processor failed to be created")
)

type AggProcessorConfig struct {
	VdiskID  string
	K        int
	M        int
	PrivKey  string
	HexNonce string
}

type AggProcessorReq struct {
	AggCh   chan AggMqMsg
	Config  AggProcessorConfig
	Context context.Context
}

type AggMqMsg []byte

type MQ struct {
	NeedProcessorCh   chan AggProcessorReq
	NeedProcessorResp chan bool
	AggChs            map[string]chan AggMqMsg
	mux               sync.Mutex
}

func NewMQ() *MQ {
	return &MQ{
		NeedProcessorCh:   make(chan AggProcessorReq),
		NeedProcessorResp: make(chan bool),
		AggChs:            make(map[string]chan AggMqMsg),
	}
}

// AskProcessor
func (mq *MQ) AskProcessor(ctx context.Context, apc AggProcessorConfig) (chan AggMqMsg, error) {
	mq.mux.Lock()
	defer mq.mux.Unlock()

	// check if the processor already existed
	if aggCh, ok := mq.AggChs[apc.VdiskID]; ok {
		return aggCh, nil
	}

	aggCh := make(chan AggMqMsg, 100)

	// ask for processor
	mq.NeedProcessorCh <- AggProcessorReq{
		AggCh:   aggCh,
		Config:  apc,
		Context: ctx,
	}

	created := <-mq.NeedProcessorResp
	if !created {
		return aggCh, ErrProcessorNotCreated
	}

	mq.AggChs[apc.VdiskID] = aggCh
	return aggCh, nil
}
