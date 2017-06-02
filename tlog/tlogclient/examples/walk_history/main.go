package main

import (
	"flag"

	blockstorcfg "github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"

	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
)

type config struct {
	K                    int
	M                    int
	privKey              string
	vdiskID              string
	nonce                string // encryption nonce
	TlogObjStorAddresses string
	ConfigPath           string
}

func main() {
	var conf config

	flag.IntVar(&conf.K, "k", 4, "K variable of erasure encoding")
	flag.IntVar(&conf.M, "m", 2, "M variable of erasure encoding")
	flag.StringVar(&conf.privKey, "priv-key", "12345678901234567890123456789012", "priv key")
	flag.StringVar(&conf.vdiskID, "vdiskid", "1234567890", "virtual disk ID")
	flag.StringVar(&conf.nonce, "nonce", "37b8e8a308c354048d245f6d", "encryption nonce")
	flag.StringVar(&conf.TlogObjStorAddresses, "storage-addresses", "",
		"comma seperated list of redis compatible connectionstrings (format: '<ip>:<port>[@<db>]', eg: 'localhost:16379,localhost:6379@2'), if given, these are used for all vdisks, ignoring the given config")
	flag.StringVar(&conf.ConfigPath, "config", "config.yml", "blockstor config file")

	flag.Parse()

	// parse optional server configs
	serverConfigs, err := blockstorcfg.ParseCSStorageServerConfigStrings(conf.TlogObjStorAddresses)
	exitOnErr(err)

	// create redisPool, used by the tlog decoder
	redisPool, err := tlog.AnyRedisPool(tlog.RedisPoolConfig{
		VdiskID:                 conf.vdiskID,
		RequiredDataServerCount: conf.K + conf.M,
		ConfigPath:              conf.ConfigPath,
		ServerConfigs:           serverConfigs,
		AutoFill:                true,
		AllowInMemory:           false,
	})

	dec, err := decoder.New(redisPool, conf.K, conf.M, conf.vdiskID, conf.privKey, conf.nonce)
	if err != nil {
		log.Fatalf("tlog decoder creation failed:%v", err)
	}
	aggChan := dec.Decode(0)

	for {
		da, more := <-aggChan
		if !more {
			break
		}
		if da.Err != nil {
			log.Fatalf("error to decode:%v", da.Err)
		}
		agg := da.Agg
		log.Info("================================")
		log.Infof("agg timestamp=%v, size=%v", agg.Timestamp(), agg.Size())

		vdiskID, err := agg.VdiskID()
		exitOnErr(err)
		blocks, err := agg.Blocks()
		exitOnErr(err)

		for i := 0; i < blocks.Len(); i++ {
			block := blocks.At(i)
			exitOnErr(err)

			data, err := block.Data()
			exitOnErr(err)

			log.Infof("seq=%v , vdiskID=%v, data=%v", block.Sequence(), vdiskID, string(data[:3]))
		}
	}
}

func exitOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
