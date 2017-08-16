package restore

import (
	"context"
	"errors"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	cmdConf "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

// vdiskCfg is the configuration used for the restore vdisk command
var vdiskCmdCfg struct {
	SourceConfig config.SourceConfig
	K, M         int
	PrivKey      string
	StartTs      uint64 // start timestamp
	EndTs        uint64 // end timestamp
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

	player, err := player.NewPlayer(ctx, configSource, vdiskID,
		vdiskCmdCfg.PrivKey, vdiskCmdCfg.K, vdiskCmdCfg.M)
	if err != nil {
		return err
	}

	_, err = player.Replay(decoder.NewLimitByTimestamp(vdiskCmdCfg.StartTs, vdiskCmdCfg.EndTs))
	return err
}

func init() {
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
	VdiskCmd.Flags().Uint64Var(
		&vdiskCmdCfg.StartTs,
		"start-timestamp", 0,
		"start timestamp in nanosecond(default 0: since beginning)")
	VdiskCmd.Flags().Uint64Var(
		&vdiskCmdCfg.EndTs,
		"end-timestamp", 0,
		"end timestamp in nanosecond(default 0: until the end)")
}
