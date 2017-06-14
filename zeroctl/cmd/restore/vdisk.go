package restore

import (
	"context"
	"errors"
	"fmt"

	zerodiskcfg "github.com/zero-os/0-Disk/config"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/gonbdserver/nbd"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/ardb"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	"github.com/zero-os/0-Disk/zeroctl/cmd/config"
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

	// parse optional server configs
	serverConfigs, err := zerodiskcfg.ParseCSStorageServerConfigStrings(vdiskCfg.TlogObjStorAddresses)
	if err != nil {
		return fmt.Errorf(
			"failed to parse given connection strings %q: %s",
			vdiskCfg.TlogObjStorAddresses, err.Error())
	}

	player, err := player.NewPlayer(ctx, vdiskCfg.ConfigPath, serverConfigs, vdiskID,
		vdiskCfg.PrivKey, vdiskCfg.HexNonce, vdiskCfg.K, vdiskCfg.M)
	if err != nil {
		return err
	}

	return player.Replay(0, 0)
}

// create a new backend, used for writing
func newBackend(ctx context.Context, dial ardb.DialFunc, tlogrpc, vdiskID, configPath string) (nbd.Backend, error) {
	// redis pool
	redisPool := ardb.NewRedisPool(dial)

	hotreloader, err := zerodiskcfg.NopHotReloader(configPath, zerodiskcfg.NBDServer)
	if err != nil {
		return nil, err
	}

	config := ardb.BackendFactoryConfig{
		Pool:              redisPool,
		ConfigHotReloader: hotreloader,
		LBACacheLimit:     ardb.DefaultLBACacheLimit,
		TLogRPCAddress:    tlogrpc,
	}
	fact, err := ardb.NewBackendFactory(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create factory:%v", err)
	}

	ec := &nbd.ExportConfig{
		Name:        vdiskID,
		Description: "zero-os/zerodisk",
		Driver:      "ardb",
		ReadOnly:    false,
		TLSOnly:     false,
	}

	return fact.NewBackend(ctx, ec)
}

func init() {
	VdiskCmd.Flags().StringVar(
		&vdiskCfg.TlogObjStorAddresses,
		"storage-addresses", "",
		"comma seperated list of redis compatible connectionstrings (format: '<ip>:<port>[@<db>]', eg: 'localhost:16379,localhost:6379@2'), if given, these are used for all vdisks, ignoring the given config")
	VdiskCmd.Flags().StringVar(
		&vdiskCfg.ConfigPath, "config", "config.yml",
		"zeroctl config file")
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
