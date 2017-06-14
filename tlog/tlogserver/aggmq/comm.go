package aggmq

const (
	_ = iota
	CmdWaitSlaveSync
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
	aggCh  chan AggMqMsg // channel of aggregation message
	cmdCh  chan Command  // channel of command
	respCh chan Response // channel of response
}

func newAggComm() *AggComm {
	return &AggComm{
		aggCh:  make(chan AggMqMsg, 100),
		cmdCh:  make(chan Command),
		respCh: make(chan Response),
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
	// TODO : add timeout on the waiting
	resp := <-comm.respCh
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
