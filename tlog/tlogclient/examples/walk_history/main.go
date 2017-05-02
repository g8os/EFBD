package main

import (
	"flag"
	"fmt"

	log "github.com/glendc/go-mini-log"

	"github.com/g8os/blockstor/tlog/tlogclient/decoder"
)

type config struct {
	K                int
	M                int
	firstObjStorPort int
	firstObjStorAddr string
	privKey          string
	vdiskID          string
	nonce            string // encryption nonce
}

func main() {
	var conf config

	flag.IntVar(&conf.K, "k", 4, "K variable of erasure encoding")
	flag.IntVar(&conf.M, "m", 2, "M variable of erasure encoding")
	flag.StringVar(&conf.firstObjStorAddr, "first-objstor-addr", "127.0.0.1", "first objstor addr")
	flag.IntVar(&conf.firstObjStorPort, "first-objstor-port", 16379, "first objstor port")
	flag.StringVar(&conf.privKey, "priv-key", "12345678901234567890123456789012", "priv key")
	flag.StringVar(&conf.vdiskID, "vdiskid", "1234567890", "virtual disk ID")
	flag.StringVar(&conf.nonce, "nonce", "37b8e8a308c354048d245f6d", "encryption nonce")

	flag.Parse()

	objstorAddrs := []string{}
	for i := 0; i < conf.K+conf.M+1; i++ {
		addr := fmt.Sprintf("%v:%v", conf.firstObjStorAddr, conf.firstObjStorPort+i)
		objstorAddrs = append(objstorAddrs, addr)
	}

	dec, err := decoder.New(objstorAddrs, conf.K, conf.M, conf.vdiskID, conf.privKey, conf.nonce)
	if err != nil {
		log.Fatalf("tlog decoder creation failed:%v", err)
	}
	aggChan, errChan := dec.Decode(0)

	finished := false
	for !finished {
		select {
		case agg := <-aggChan:
			log.Info("================================")
			log.Infof("agg timestamp=%v, size=%v", agg.Timestamp(), agg.Size())
			blocks, err := agg.Blocks()
			exitOnErr(err)

			for i := 0; i < blocks.Len(); i++ {
				block := blocks.At(i)
				vdiskID, err := block.VdiskID()
				exitOnErr(err)

				data, err := block.Data()
				exitOnErr(err)

				log.Infof("block %v , vdiskID=%v, data=%v", i, vdiskID, string(data[:3]))
			}
		case err := <-errChan:
			log.Infof("err=%v", err)
			finished = true
		}
	}
}

func exitOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
