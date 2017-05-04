package main

import (
	"context"
	"flag"
	"net/http/httptest"
	"os"
	"strings"

	log "github.com/glendc/go-mini-log"

	"github.com/g8os/blockstor/gridapi"
	"github.com/g8os/blockstor/nbdserver/ardb"
	"github.com/g8os/blockstor/storagecluster"
	"github.com/g8os/blockstor/tlog/tlogclient/decoder"
	"github.com/g8os/gonbdserver/nbd"
)

func main() {
	var gridapiaddress string
	var nonDedupedExports string
	var testArdbConnectionStrings string
	var vdiskID string
	var tlogObjstorAddrs string
	var K, M int
	var privKey, hexNonce string

	flag.StringVar(&gridapiaddress, "gridapi", "", "Address of the grid api REST API, leave empty to use the embedded stub")
	flag.StringVar(&nonDedupedExports, "nondeduped", "", "when using the embedded gridapi, comma seperated list of exports that should not be deduped")
	flag.StringVar(&testArdbConnectionStrings, "testardbs", "localhost:16377,localhost:16378", "Comma seperated list of ardb connection strings returned by the embedded backend controller, first one is the metadataserver")
	flag.StringVar(&vdiskID, "vdiskid", "default", "vdisk ID")
	flag.StringVar(&tlogObjstorAddrs, "tlog-objstor-addrs",
		"127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381,127.0.0.1:16382,127.0.0.1:16383,127.0.0.1:16384,127.0.0.1:16385",
		"tlog objstor addrs")
	flag.IntVar(&K, "k", 4, "K variable of erasure encoding")
	flag.IntVar(&M, "m", 2, "M variable of erasure encoding")
	flag.StringVar(&privKey, "priv-key", "12345678901234567890123456789012", "private key")
	flag.StringVar(&hexNonce, "nonce", "37b8e8a308c354048d245f6d", "hex nonce used for encryption")

	flag.Parse()

	// gridapiaddress
	if gridapiaddress == "" {
		log.Info("Starting embedded grid api")
		var s *httptest.Server
		var err error
		s, gridapiaddress, err = gridapi.NewGridAPIServer(testArdbConnectionStrings,
			strings.Split(nonDedupedExports, ","), 20)
		if err != nil {
			log.Fatal(err)
		}
		defer s.Close()
	}
	log.Info("Using grid api at", gridapiaddress)

	// redis pool
	var poolDial ardb.DialFunc
	redisPool := ardb.NewRedisPool(poolDial)

	// storage cluster
	storageClusterClientFactory, err := storagecluster.NewClusterClientFactory(
		gridapiaddress, log.New(os.Stderr, "storagecluster:", log.Flags()))
	if err != nil {
		log.Fatalf("failed to create storageClusterClientFactory:%v", err)
	}
	go storageClusterClientFactory.Listen(context.TODO())

	config := ardb.BackendFactoryConfig{
		Pool:            redisPool,
		GridAPIAddress:  gridapiaddress,
		LBACacheLimit:   ardb.DefaultLBACacheLimit,
		SCClientFactory: storageClusterClientFactory,
		//RootARDBConnectionString: rootArdbConnectionSttring,
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

	backendCtx := context.TODO()
	backend, err := fact.NewBackend(backendCtx, ec)
	if err != nil {
		log.Fatalf("failed to create backend:%v", err)
	}
	log.Infof("have fun!!!")

	// create tlog decoder
	addrs := strings.Split(tlogObjstorAddrs, ",")
	log.Infof("addr=%v", addrs)
	dec, err := decoder.New(strings.Split(tlogObjstorAddrs, ","), K, M, vdiskID, privKey, hexNonce)
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
			data, err := block.Data()
			if err != nil {
				log.Fatalf("failed to get data block of lba=%v, err=%v", lba, err)
			}
			backend.WriteAt(backendCtx, data, int64(lba), false)
		}
	}
}
