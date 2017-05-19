package restore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	blockstorcfg "github.com/g8os/blockstor/config"

	"github.com/g8os/blockstor/g8stor/cmd/config"
	"github.com/g8os/blockstor/gonbdserver/nbd"
	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/nbdserver/ardb"
	"github.com/g8os/blockstor/storagecluster"
	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogclient/decoder"
	"github.com/spf13/cobra"
)

var vdiskCfg struct {
	TlogObjStorAddresses string
	ConfigPath           string
	K, M                 int
	PrivKey, HexNonce    string
}

// VdiskCmd represents the restore vdisk subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisk id",
	Short: "Restore a vdisk using a given tlogserver",
	RunE:  restoreVdisk,
}

func restoreVdisk(cmd *cobra.Command, args []string) error {
	argn := len(args)

	if argn < 1 {
		return errors.New("not enough arguments")
	}
	if argn > 1 {
		return errors.New("too many arguments")
	}

	vdiskID := args[0]

	// redis pool
	var poolDial ardb.DialFunc
	redisPool := ardb.NewRedisPool(poolDial)

	logLevel := log.ErrorLevel
	if config.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	ctx := context.Background()

	// storage cluster
	storageClusterClientFactory, err := storagecluster.NewClusterClientFactory(
		vdiskCfg.ConfigPath, log.New("storagecluster", logLevel))
	if err != nil {
		return fmt.Errorf("failed to create storageClusterClientFactory:%v", err)
	}
	go storageClusterClientFactory.Listen(ctx)

	config := ardb.BackendFactoryConfig{
		Pool:            redisPool,
		ConfigPath:      vdiskCfg.ConfigPath,
		LBACacheLimit:   ardb.DefaultLBACacheLimit,
		SCClientFactory: storageClusterClientFactory,
	}
	fact, err := ardb.NewBackendFactory(config)
	if err != nil {
		return fmt.Errorf("failed to create factory:%v", err)
	}

	ec := &nbd.ExportConfig{
		Name:        vdiskID,
		Description: "Deduped g8os blockstor",
		Driver:      "ardb",
		ReadOnly:    false,
		TLSOnly:     false,
	}

	backend, err := fact.NewBackend(ctx, ec)
	if err != nil {
		return fmt.Errorf("failed to create backend:%v", err)
	}

	// parse optional server configs
	serverConfigs, err := blockstorcfg.ParseCSStorageServerConfigStrings(vdiskCfg.TlogObjStorAddresses)
	if err != nil {
		return fmt.Errorf(
			"failed to parse given connection strings %q: %s",
			vdiskCfg.TlogObjStorAddresses, err.Error())
	}

	// create redisPool, used by the tlog decoder
	tlogRedisPool, err := tlog.AnyRedisPool(tlog.RedisPoolConfig{
		VdiskID:                 vdiskID,
		RequiredDataServerCount: vdiskCfg.K + vdiskCfg.M,
		ConfigPath:              vdiskCfg.ConfigPath,
		ServerConfigs:           serverConfigs,
		AutoFill:                true,
		AllowInMemory:           false,
	})

	// create tlog decoder
	addrs := strings.Split(vdiskCfg.TlogObjStorAddresses, ",")
	log.Infof("addr=%v", addrs)
	dec, err := decoder.New(
		tlogRedisPool,
		vdiskCfg.K, vdiskCfg.M,
		vdiskID,
		vdiskCfg.PrivKey, vdiskCfg.HexNonce)
	if err != nil {
		return fmt.Errorf("failed to create tlog decoder:%v", err)
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
			return fmt.Errorf("failed to get vdisk id from aggregation:%v", err)
		}
		if strings.Compare(storedViskID, vdiskID) != 0 {
			return fmt.Errorf("vdisk id not mactched .expected=%v, got=%v", vdiskID, storedViskID)
		}

		blocks, err := agg.Blocks()
		for i := 0; i < blocks.Len(); i++ {
			block := blocks.At(i)
			lba := block.Lba()

			switch block.Operation() {
			case schema.OpWrite:
				data, err := block.Data()
				if err != nil {
					return fmt.Errorf("failed to get data block of lba=%v, err=%v", lba, err)
				}
				if _, err := backend.WriteAt(ctx, data, int64(lba)); err != nil {
					return fmt.Errorf("failed to WriteAt lba=%v, err=%v", lba, err)
				}
			case schema.OpWriteZeroesAt:
				if _, err := backend.WriteZeroesAt(ctx, int64(lba), int64(block.Size())); err != nil {
					return fmt.Errorf("failed to WriteAt lba=%v, err=%v", lba, err)
				}
			}
		}
	}

	return backend.Flush(ctx)
}

func init() {
	VdiskCmd.Flags().StringVar(
		&vdiskCfg.TlogObjStorAddresses,
		"storage-addresses", "",
		"comma seperated list of redis compatible connectionstrings (format: '<ip>:<port>[@<db>]', eg: 'localhost:16379,localhost:6379@2'), if given, these are used for all vdisks, ignoring the given config")
	VdiskCmd.Flags().StringVar(
		&vdiskCfg.ConfigPath, "config", "config.yml",
		"blockstor config file")
	VdiskCmd.Flags().IntVar(
		&vdiskCfg.K,
		"k", 4,
		"K variable of erasure encoding")
	VdiskCmd.Flags().IntVar(
		&vdiskCfg.M,
		"m", 2,
		"M variable of erasure encoding")
	VdiskCmd.Flags().StringVar(
		&vdiskCfg.PrivKey,
		"priv-key", "12345678901234567890123456789012",
		"private key")
	VdiskCmd.Flags().StringVar(
		&vdiskCfg.HexNonce,
		"nonce", "37b8e8a308c354048d245f6d",
		"hex nonce used for encryption")
}
