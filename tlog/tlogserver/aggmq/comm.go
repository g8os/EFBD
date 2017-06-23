package aggmq

import (
	"errors"
	"time"
)

const (
	_ = iota
	CmdWaitSlaveSync
	CmdRestartSlaveSyncer
	CmdKillMe
)

var (
	// ErrSendCmdTimeout returned when the command doesn't get any response after
	// some amount of time
	ErrSendCmdTimeout = errors.New("send command timed out")
)

// AggMqMsg defines message given to the aggregation processor
type AggMqMsg []byte

// Command defines command from producer to consumer/processor
type Command struct {
	Type int
	Seq  uint64
}

// Response defines response from consumer/processor to producer
type Response struct {
	Err error
}

// AggComm defines communication channels
// between aggregation producer and consumer/processor
type AggComm struct {
	aggCh   chan AggMqMsg // channel of aggregation message
	cmdCh   chan Command  // channel of command
	respCh  chan Response // channel of response
	mq      *MQ
	vdiskID string
}

func newAggComm(mq *MQ, vdiskID string) *AggComm {
	return &AggComm{
		aggCh:   make(chan AggMqMsg, 100),
		cmdCh:   make(chan Command),
		respCh:  make(chan Response),
		mq:      mq,
		vdiskID: vdiskID,
	}
}

// SendAgg send aggregation via this comm object
func (comm *AggComm) SendAgg(agg AggMqMsg) {
	comm.aggCh <- agg
}

// RecvAgg recv aggregation from this comm object
func (comm *AggComm) RecvAgg() <-chan AggMqMsg {
	return comm.aggCh
}

// SendCmd send command using this comm object
func (comm *AggComm) SendCmd(cmd int, seq uint64) error {
	// send command
	comm.cmdCh <- Command{
		Type: cmd,
		Seq:  seq,
	}

	// wait for the response
	var resp Response

	select {
	case resp = <-comm.respCh:

	case <-time.After(5 * time.Second):
		return ErrSendCmdTimeout
	}
	return resp.Err
}

// RecvCmd receive command from this comm object
func (comm *AggComm) RecvCmd() <-chan Command {
	return comm.cmdCh
}

// SendResp send response using this comm object
func (comm *AggComm) SendResp(err error) {
	comm.respCh <- Response{Err: err}
}

// Destroy destroys this communication channel.
// It also means destroying the consumer
func (comm *AggComm) Destroy() {
	comm.cmdCh <- Command{
		Type: CmdKillMe,
	}
	comm.mq.deleteProcessor(comm.vdiskID)
}
