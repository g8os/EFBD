# TLOG Client

Tlog client is asynchronous tlog client library. It is currently not goroutine safe.

## API

The main APIs are : `New`, `Send` and `Recv`

[New](https://godoc.org/github.com/g8os/blockstor/tlog/tlogclient#New) to create new client instance.
[Send](https://godoc.org/github.com/g8os/blockstor/tlog/tlogclient#Client.Send) to send transaction to server.
[Recv](https://godoc.org/github.com/g8os/blockstor/tlog/tlogclient#Client.Recv) to get the channel to receive the server
response.

## usage example

```

package main

import (
	"flag"
	"io"
	"sync"
	"time"

	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog"
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

	numLogsToSend := 25 * numFlush
	logsToSend := map[uint64]struct{}{}
	for i := 0; i < numLogsToSend; i++ {
		logsToSend[uint64(i)] = struct{}{}
	}

	var wg sync.WaitGroup

	// start the response receiver goroutine
	// wait for all sequences to be flushed
	wg.Add(1)
	go func() {
		defer wg.Done()

		respChan := client.Recv()
		for {
			r := <-respChan
			if r.Err != nil {
				if r.Err == io.EOF {
					continue
				}
				log.Fatalf("resp error:%v", r.Err)
			}
			resp := r.Resp
			if printResp {
				log.Infof("status=%v, seqs=%v\n", resp.Status, resp.Sequences)
			}

			if resp.Status == tlog.BlockStatusFlushOK {
				for _, seq := range resp.Sequences {
					delete(logsToSend, seq)
				}
				if len(logsToSend) == 0 {
					return
				}
			}
		}
	}()

	// send the data
	for i := 0; i < numLogsToSend; i++ {
		seq := uint64(i)
		err := client.Send(schema.OpWrite, seq, seq*dataLen, uint64(time.Now().Unix()), data, uint64(len(data)))
		if err != nil {
			log.Fatalf("send failed at seq=%v, err= %v", seq, err)
			return
		}
	}

	wg.Wait() //wait until we receive all the response
}
```
