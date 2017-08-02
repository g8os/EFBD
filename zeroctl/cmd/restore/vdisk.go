package restore

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	cmdConf "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

// vdiskCfg is the configuration used for the restore vdisk command
var vdiskCmdCfg struct {
	TlogObjStorAddresses string
	SourceConfig         config.SourceConfig
	K, M                 int
	PrivKey, HexNonce    string
	StartTs              uint64 // start timestamp
	EndTs                uint64 // end timestamp
}

// VdiskCmd represents the restore vdisk subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisk id",
	Short: "Restore a vdisk using a given tlogserver",
	RunE:  restoreVdisk,
}

func restoreVdisk(cmd *cobra.Command, args []string) error {
	// create config source
	configSource, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer configSource.Close()

	// parse positional args
	argn := len(args)
	if argn < 1 {
		return errors.New("not enough arguments")
	}
	if argn > 1 {
		return errors.New("too many arguments")
	}

	vdiskID := args[0]

	logLevel := log.ErrorLevel
	if cmdConf.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	ctx := context.Background()

	// parse optional server configs
	serverConfigs, err := config.ParseCSStorageServerConfigStrings(vdiskCmdCfg.TlogObjStorAddresses)
	if err != nil {
		return fmt.Errorf(
			"failed to parse given connection strings %q: %s",
			vdiskCmdCfg.TlogObjStorAddresses, err.Error())
	}

	player, err := player.NewPlayer(ctx, configSource, serverConfigs, vdiskID,
		vdiskCmdCfg.PrivKey, vdiskCmdCfg.HexNonce, vdiskCmdCfg.K, vdiskCmdCfg.M)
	if err != nil {
		return err
	}

	_, err = player.Replay(decoder.NewLimitByTimestamp(vdiskCmdCfg.StartTs, vdiskCmdCfg.EndTs))
	return err
}

func init() {
	VdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.TlogObjStorAddresses,
		"storage-addresses", "",
		"comma seperated list of redis compatible connectionstrings (format: '<ip>:<port>[@<db>]', eg: 'localhost:16379,localhost:6379@2'), if given, these are used for all vdisks, ignoring the given config")
	VdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")
	VdiskCmd.Flags().IntVar(
		&vdiskCmdCfg.K,
		"k", 4,
		"K variable of erasure encoding")
	VdiskCmd.Flags().IntVar(
		&vdiskCmdCfg.M,
		"m", 2,
		"M variable of erasure encoding")
	VdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.PrivKey,
		"priv-key", "12345678901234567890123456789012",
		"private key")
	VdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.HexNonce,
		"nonce", "37b8e8a308c354048d245f6d",
		"hex nonce used for encryption")
	VdiskCmd.Flags().Uint64Var(
		&vdiskCmdCfg.StartTs,
		"start-timestamp", 0,
		"start timestamp in nanosecond(default 0: since beginning)")
	VdiskCmd.Flags().Uint64Var(
		&vdiskCmdCfg.EndTs,
		"end-timestamp", 0,
		"end timestamp in nanosecond(default 0: until the end)")
}
