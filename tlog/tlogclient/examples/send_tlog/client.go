package main

import (
	"flag"
	"io"
	"sync"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
	client "github.com/zero-os/0-Disk/tlog/tlogclient"
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
		vdiskID       = "myimg"
		firstSequence = 0
		dataLen       = 4096
	)

	client, err := client.New([]string{"127.0.0.1:11211"}, vdiskID)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	data := make([]byte, dataLen)
	for i := 0; i < dataLen; i++ {
		data[i] = 'a'
	}
	data[0] = 'b'
	data[1] = 'c'

	numLogsToSend := 25 * numFlush
	logsToSend := map[uint64]struct{}{}
	for i := 0; i < numLogsToSend; i++ {
		logsToSend[uint64(i)+1] = struct{}{}
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
		seq := uint64(i) + 1
		err := client.Send(schema.OpSet, seq, int64(seq), tlog.TimeNowTimestamp(), data)
		if err != nil {
			log.Fatalf("send failed at seq=%v, err= %v", seq, err)
			return
		}
	}

	wg.Wait() //wait until we receive all the response
}
