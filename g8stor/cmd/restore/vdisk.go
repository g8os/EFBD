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

	logLevel := log.ErrorLevel
	if config.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	ctx := context.Background()

	// create nbd backend
	backend, err := newBackend(ctx, nil, "", vdiskID, vdiskCfg.ConfigPath)
	if err != nil {
		return err
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

	// replay tlog core logic:
	// decode data from tlogserver and write it to the nbd's backend
	return decode(
		ctx, backend, tlogRedisPool, vdiskID,
		vdiskCfg.K, vdiskCfg.M, vdiskCfg.PrivKey, vdiskCfg.HexNonce)
}

// create a new backend, used for writing
func newBackend(ctx context.Context, dial ardb.DialFunc, tlogrpc, vdiskID, configPath string) (nbd.Backend, error) {
	// redis pool
	redisPool := ardb.NewRedisPool(nil)

	// storage cluster
	storageClusterClientFactory, err := storagecluster.NewClusterClientFactory(
		configPath, log.New("storagecluster", log.GetLevel()))
	if err != nil {
		return nil, fmt.Errorf("failed to create storageClusterClientFactory:%v", err)
	}
	go storageClusterClientFactory.Listen(ctx)

	config := ardb.BackendFactoryConfig{
		Pool:            redisPool,
		ConfigPath:      configPath,
		LBACacheLimit:   ardb.DefaultLBACacheLimit,
		SCClientFactory: storageClusterClientFactory,
		TLogRPCAddress:  tlogrpc,
	}
	fact, err := ardb.NewBackendFactory(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create factory:%v", err)
	}

	ec := &nbd.ExportConfig{
		Name:        vdiskID,
		Description: "g8os blockstor",
		Driver:      "ardb",
		ReadOnly:    false,
		TLSOnly:     false,
	}

	return fact.NewBackend(ctx, ec)
}

// replay tlog by decoding data from a tlog RedisPool
// and writing that data into the nbdserver
func decode(ctx context.Context, backend nbd.Backend, pool tlog.RedisPool, vdiskID string, k, m int, privKey, hexNonce string) error {
	// create tlog decoder
	dec, err := decoder.New(pool, k, m, vdiskID, privKey, hexNonce)
	if err != nil {
		return fmt.Errorf("failed to create tlog decoder: %v", err)
	}

	aggChan := dec.Decode(0)
	for {
		da, more := <-aggChan
		if !more {
			break
		}

		if da.Err != nil {
			return fmt.Errorf("failed to get aggregation: %v", da.Err)
		}

		agg := da.Agg

		// some small checking
		storedViskID, err := agg.VdiskID()
		if err != nil {
			return fmt.Errorf("failed to get vdisk id from aggregation: %v", err)
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
