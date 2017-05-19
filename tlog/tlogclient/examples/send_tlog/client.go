package main

import (
	"flag"
	"sync"
	"time"

	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog/schema"
	client "github.com/g8os/blockstor/tlog/tlogclient"
)

var (
	numFlush  int
	printResp bool
)

func main() {
	flag.IntVar(&numFlush, "num_flush", 40, "number of flush")
	flag.BoolVar(&printResp, "print_resp", false, "print response")

	flag.Parse()

	const (
		vdiskID       = "1234567890"
		firstSequence = 0
		dataLen       = 4096
	)

	client, err := client.New("127.0.0.1:11211", vdiskID, firstSequence)
	if err != nil {
		log.Fatal(err)
	}

	data := make([]byte, dataLen)
	for i := 0; i < dataLen; i++ {
		data[i] = 'a'
	}
	data[0] = 'b'
	data[1] = 'c'

	logsToSend := 25 * numFlush

	var wg sync.WaitGroup

	// start the response receiver goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		respChan := client.Recv(1)
		expectedResp := logsToSend /*response for each mesage */ + numFlush /* flush response */

		for i := 0; i < expectedResp; i++ {
			resp := <-respChan
			if resp.Err != nil {
				log.Fatalf("resp error:%v", resp.Err)
			}
			if printResp {
				log.Infof("status=%v, seqs=%v\n", resp.Resp.Status, resp.Resp.Sequences)
			}
		}
	}()

	// send the data
	for i := 0; i < logsToSend; i++ {
		seq := uint64(i)
		err := client.Send(schema.OpWrite, seq, seq*dataLen, uint64(time.Now().Unix()), data, uint64(len(data)))
		if err != nil {
			log.Fatalf("send failed at seq=%v, err= %v", seq, err)
			return
		}
	}

	wg.Wait() //wait until we receive all the response
}
