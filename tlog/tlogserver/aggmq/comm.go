package aggmq

// AggMqMsg defines message given to the aggregation processor
type AggMqMsg []byte

// Command defines command from producer to consumer/processor
type Command struct {
}

// Response defines response from consumer/processor to producer
type Response struct {
}

// AggComm defines communication channels
// between aggregation producer and consumer/processor
type AggComm struct {
	AggCh  chan AggMqMsg // channel of aggregation message
	CmdCh  chan Command  // channel of command
	RespCh chan Response // channel of response
}

func newAggComm() *AggComm {
	return &AggComm{
		AggCh:  make(chan AggMqMsg, 100),
		CmdCh:  make(chan Command),
		RespCh: make(chan Response),
	}
}

func (comm *AggComm) SendAgg(agg AggMqMsg) {
	comm.AggCh <- agg
}

func (comm *AggComm) RecvAgg() <-chan AggMqMsg {
	return comm.AggCh
}
