package main

import (
	"flag"

	"github.com/g8os/blockstor/tlog/schema"
	client "github.com/g8os/blockstor/tlog/tlogclient"
	log "github.com/glendc/go-mini-log"
)

var (
	numClient int
	numFlush  int
	printResp bool
)

func main() {
	flag.IntVar(&numClient, "num_client", 2, "number of clients")
	flag.IntVar(&numFlush, "num_flush", 40, "number of flush")
	flag.BoolVar(&printResp, "print_resp", false, "print response")

	flag.Parse()

	const (
		vdiskID = "1234567890"
	)

	clients := make([]*client.Client, numClient)
	clientReady := make(chan int, numClient)
	seqChan := make(chan uint64, 8)

	for i := 0; i < numClient; i++ {
		client, err := client.New("127.0.0.1:11211")
		if err != nil {
			log.Fatal(err)
		}
		clients[i] = client
		clientReady <- i
	}
	data := make([]byte, 4096*4)
	for i := 0; i < (4096 * 4); i++ {
		data[i] = 'a'
	}
	data[0] = 'b'
	data[1] = 'c'

	// produce the data
	go func() {
		for i := 0; i < 25*numFlush; i++ {
			seqChan <- uint64(i)
		}
	}()

	for seq := range seqChan {
		idx := <-clientReady
		func(j uint64, idx int) {
			client := clients[int(j)%numClient]

			log.Infof("j=%v\n", j)
			err := client.Send(vdiskID, schema.OpWrite, j, j, j, data, uint64(len(data)))
			if err != nil {
				log.Infof("client %v died\n", idx)
				return
			}
			tr, err := client.RecvOne()
			if err != nil {
				log.Fatalf("client %v failed to recv:%v\n", idx, err)
			}
			if printResp {
				log.Infof("status=%v, seqs=%v\n", tr.Status, tr.Sequences)
			}
			clientReady <- idx
		}(seq, idx)
		if int(seq)+1 == 25*numFlush {
			return
		}
	}
}
