package delvdisk

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCmdCfg struct {
	SourceConfig config.SourceConfig
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

	// create config source
	source, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer source.Close()

	argn := len(args)
	if argn < 1 {
		return errors.New("no vdisk identifier given")
	}
	if argn > 1 {
		return errors.New("too many vdisk identifier given")
	}
	vdiskID := args[0]

	// get vdisk and cluster config
	staticCfg, err := config.ReadVdiskStaticConfig(source, vdiskID)
	if err != nil {
		return err
	}
	nbdConfig, err := config.ReadVdiskNBDConfig(source, vdiskID)
	if err != nil {
		return err
	}
	clusterConfig, err := config.ReadStorageClusterConfig(source, nbdConfig.StorageClusterID)
	if err != nil {
		return err
	}

	cluster, err := ardb.NewCluster(*clusterConfig, nil)
	if err != nil {
		return err
	}

	_, err = storage.DeleteVdisk(vdiskID, staticCfg.Type, cluster)
	return err
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
}
