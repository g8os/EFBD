package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/g8os/blockstor/gonbdserver/nbd"
	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/nbdserver/ardb"
	"github.com/g8os/blockstor/storagecluster"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogclient/decoder"
)

func main() {
	var tlogObjstorAddrs string
	var configPath string
	var K, M int
	var privKey, hexNonce string

	flag.StringVar(&tlogObjstorAddrs, "tlog-objstor-addrs",
		"127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381,127.0.0.1:16382,127.0.0.1:16383,127.0.0.1:16384,127.0.0.1:16385",
		"tlog objstor addrs")
	flag.StringVar(&configPath, "config", "config.yml", "ARDB Config YAML File")
	flag.IntVar(&K, "k", 4, "K variable of erasure encoding")
	flag.IntVar(&M, "m", 2, "M variable of erasure encoding")
	flag.StringVar(&privKey, "priv-key", "12345678901234567890123456789012", "private key")
	flag.StringVar(&hexNonce, "nonce", "37b8e8a308c354048d245f6d", "hex nonce used for encryption")

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, "Restore a vdisk using the transactions stored in the tlogserver.\n\n")
		fmt.Fprintf(os.Stderr, "usage: %s vdiskID\n\nOptional Flags:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprint(os.Stderr, "  -h, -help\n    \tprint this usage message\n")
	}

	flag.Parse()

	posArgs := flag.Args()
	if len(posArgs) != 1 {
		fmt.Fprint(os.Stderr, "ERROR: only one positional argument (vdiskID) is required and allowed\n\n")
		flag.Usage()
		os.Exit(2)
	}
	vdiskID := posArgs[0]

	// redis pool
	var poolDial ardb.DialFunc
	redisPool := ardb.NewRedisPool(poolDial)

	// storage cluster
	storageClusterClientFactory, err := storagecluster.NewClusterClientFactory(
		configPath, log.New("storagecluster", log.InfoLevel))
	if err != nil {
		log.Fatalf("failed to create storageClusterClientFactory:%v", err)
	}
	go storageClusterClientFactory.Listen(context.Background())

	config := ardb.BackendFactoryConfig{
		Pool:            redisPool,
		ConfigPath:      configPath,
		LBACacheLimit:   ardb.DefaultLBACacheLimit,
		SCClientFactory: storageClusterClientFactory,
	}
	fact, err := ardb.NewBackendFactory(config)
	if err != nil {
		log.Fatalf("failed to create factory:%v", err)
	}

	ec := &nbd.ExportConfig{
		Name:        vdiskID,
		Description: "Deduped g8os blockstor",
		Driver:      "ardb",
		ReadOnly:    false,
		TLSOnly:     false,
	}

	backendCtx := context.Background()
	backend, err := fact.NewBackend(backendCtx, ec)
	if err != nil {
		log.Fatalf("failed to create backend:%v", err)
	}

	// create tlog decoder
	addrs := strings.Split(tlogObjstorAddrs, ",")
	log.Infof("addr=%v", addrs)
	dec, err := decoder.New(addrs, K, M, vdiskID, privKey, hexNonce)
	if err != nil {
		log.Fatalf("failed to create tlog decoder:%v", err)
	}

	aggChan := dec.Decode(0)
	for {
		da, more := <-aggChan
		if !more {
			break
		}
		agg := da.Agg

		// some small checking
		storedViskID, err := agg.VdiskID()
		if err != nil {
			log.Fatalf("failed to get vdisk id from aggregation:%v", err)
		}
		if strings.Compare(storedViskID, vdiskID) != 0 {
			log.Fatalf("vdisk id not mactched .expected=%v, got=%v", vdiskID, storedViskID)
		}

		blocks, err := agg.Blocks()
		for i := 0; i < blocks.Len(); i++ {
			block := blocks.At(i)
			lba := block.Lba()

			switch block.Operation() {
			case schema.OpWrite:
				data, err := block.Data()
				if err != nil {
					log.Fatalf("failed to get data block of lba=%v, err=%v", lba, err)
				}
				if _, err := backend.WriteAt(backendCtx, data, int64(lba)); err != nil {
					log.Fatalf("failed to WriteAt lba=%v, err=%v", lba, err)
				}
			case schema.OpWriteZeroesAt:
				if _, err := backend.WriteZeroesAt(backendCtx, int64(lba), int64(block.Size())); err != nil {
					log.Fatalf("failed to WriteAt lba=%v, err=%v", lba, err)
				}
			}
		}
	}
}
