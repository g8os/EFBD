package restore

import (
	"context"
	"errors"
	"fmt"

	zerodiskcfg "github.com/zero-os/0-Disk/config"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	"github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

// vdiskCfg is the configuration used for the restore vdisk command
var vdiskCfg struct {
	TlogObjStorAddresses string
	ConfigPath           string
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

	_, err = player.Replay(decoder.NewLimitByTimestamp(vdiskCfg.StartTs, vdiskCfg.EndTs))
	return err
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
	VdiskCmd.Flags().Uint64Var(
		&vdiskCfg.StartTs,
		"start-timestamp", 0,
		"start timestamp in nanosecond(default 0: since beginning)")

	VdiskCmd.Flags().Uint64Var(
		&vdiskCfg.EndTs,
		"end-timestamp", 0,
		"end timestamp in nanosecond(default 0: until the end)")
}
