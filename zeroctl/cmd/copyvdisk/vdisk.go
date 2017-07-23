package copyvdisk

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCfg struct {
	ConfigPath string
}

// VdiskCmd represents the vdisk copy subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisk source_vdiskid target_vdiskid [target_cluster]",
	Short: "Copy a vdisk configured in the config file",
	RunE:  copyVdisk,
}

func copyVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	log.Info("parsing positional arguments...")

	// validate pos arg length
	argn := len(args)
	if argn < 2 {
		return errors.New("not enough arguments")
	} else if argn > 3 {
		return errors.New("too many arguments")
	}

	// store pos arguments in named variables
	sourceVdiskID, targetVdiskID := args[0], args[1]
	var targetStorageCluster string
	if argn == 3 {
		targetStorageCluster = args[2]
	}

	log.Infof("loading config %s...", vdiskCfg.ConfigPath)

	cfg, err := config.ReadConfig(vdiskCfg.ConfigPath, config.NBDServer)
	if err != nil {
		return err
	}

	var sourceClusterConfig, targetClusterConfig *config.StorageClusterConfig

	// get source vdisk and storage cluster
	// when the vdisk exists, it is guaranteed that its storage cluster exists to,
	// as this is validated by the config module
	sourceVdisk, ok := cfg.Vdisks[sourceVdiskID]
	if !ok {
		return fmt.Errorf("vdisk %s could not be found in config %s",
			sourceVdiskID, vdiskCfg.ConfigPath)
	}

	cluster := cfg.StorageClusters[sourceVdisk.StorageCluster]
	sourceClusterConfig = &cluster

	// if the targetStorageCluster is given,
	// try to find it its configuration in the config,
	// but as it is given by the user, it might not exist
	if targetStorageCluster != "" {
		cluster, ok := cfg.StorageClusters[targetStorageCluster]
		if !ok {
			return fmt.Errorf("targetCluster %v does not exist", targetStorageCluster)
		}
		targetClusterConfig = &cluster
	}

	// copy the vdisk
	switch stype := sourceVdisk.StorageType(); stype {
	case config.StorageDeduped:
		return storage.CopyDeduped(
			sourceVdiskID, targetVdiskID,
			sourceClusterConfig, targetClusterConfig)
	case config.StorageNonDeduped:
		return storage.CopyNonDeduped(
			sourceVdiskID, targetVdiskID,
			sourceClusterConfig, targetClusterConfig)
	case config.StorageSemiDeduped:
		return storage.CopySemiDeduped(
			sourceVdiskID, targetVdiskID,
			sourceClusterConfig, targetClusterConfig)
	default:
		return fmt.Errorf("vdisk %s has an unknown storage type %d",
			sourceVdiskID, stype)
	}
}

func init() {
	VdiskCmd.Long = VdiskCmd.Short + `

If no target storage cluster is given,
the storage cluster configured for the source vdisk
will also be used for the target vdisk.

If an error occured, the target vdisk should be considered as non-existent,
even though data which is already copied is not rolled back.

NOTE: by design,
  only the metadata of a deduped vdisk is copied,
  the data will be copied the first time the vdisk spins up,
  on the condition that the templateStorageCluster has been configured.

WARNING: when copying nondeduped vdisks,
  it is currently not supported that the target vdisk's data cluster
  has more or less storage servers, then the source vdisk's data cluster.
  See issue #206 for more information.
`

	VdiskCmd.Flags().StringVar(
		&vdiskCfg.ConfigPath, "config", "config.yml",
		"zeroctl config file")
}
