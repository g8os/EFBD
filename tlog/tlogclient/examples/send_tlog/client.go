package main

import (
	"flag"

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
	)

	seqChan := make(chan uint64, 8)

	client, err := client.New("127.0.0.1:11211", vdiskID, firstSequence)
	if err != nil {
		log.Fatal(err)
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
		func(j uint64) {
			log.Infof("j=%v\n", j)
			err := client.Send(schema.OpWrite, j, j, j, data, uint64(len(data)))
			if err != nil {
				log.Info("client died\n")
				return
			}
			tr, err := client.RecvOne()
			if err != nil {
				log.Fatalf("client failed to recv:%v\n", err)
			}
			if printResp {
				log.Infof("status=%v, seqs=%v\n", tr.Status, tr.Sequences)
			}
		}(seq)
		if int(seq)+1 == 25*numFlush {
			return
		}
	}
}
