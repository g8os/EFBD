package delvdisk

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	tlogdelete "github.com/zero-os/0-Disk/tlog/delete"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCmdCfg struct {
	SourceConfig config.SourceConfig
	PrivKey      string
}

// VdiskCmd represents the vdisks delete subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisk vdiskid",
	Short: "Delete a vdisk",
	RunE:  deleteVdisk,
}

func deleteVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.InfoLevel
	if cmdconfig.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	argn := len(args)
	if argn < 1 {
		return errors.New("no vdisk identifier given")
	}
	if argn > 1 {
		return errors.New("too many vdisk identifier given")
	}
	vdiskID := args[0]

	// create config source
	cs, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer cs.Close()
	configSource := config.NewOnceSource(cs)

	_, err = storage.DeleteVdisk(vdiskID, configSource)
	if err != nil {
		return err
	}

	return tlogdelete.Delete(configSource, vdiskID, vdiskCmdCfg.PrivKey)
}

func init() {
	VdiskCmd.Long = VdiskCmd.Short + `

WARNING: until issue #88 has been resolved,
  only the metadata of deduped vdisks can be deleted by this command.
  Nondeduped vdisks have no metadata, and thus are not affected by this issue.
`

	VdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")

	VdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.PrivKey,
		"priv-key", "12345678901234567890123456789012",
		"32 bytes tlog private key")

}
