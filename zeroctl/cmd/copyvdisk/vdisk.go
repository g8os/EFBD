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

var vdiskCmdCfg struct {
	SourceConfig            config.SourceConfig
	ForceSameStorageCluster bool
}

// VdiskCmd represents the vdisk copy subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisk source_vdiskid target_vdiskid [target_clusterid]",
	Short: "Copy a vdisk configured in the config file",
	RunE:  copyVdisk,
}

func copyVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	// create config source
	configSource, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer configSource.Close()

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
	var targetClusterID string
	if argn == 3 {
		targetClusterID = args[2]
	}

	var sourceClusterConfig, targetClusterConfig *config.StorageClusterConfig

	// try to read the Vdisk+NBD config of source vdisk
	sourceStaticConfig, err := config.ReadVdiskStaticConfig(configSource, sourceVdiskID)
	if err != nil {
		return fmt.Errorf(
			"couldn't read source vdisk %s's static config: %v", sourceVdiskID, err)
	}
	sourceStorageConfig, err := config.ReadNBDStorageConfig(
		configSource, sourceVdiskID, sourceStaticConfig)
	if err != nil {
		return fmt.Errorf(
			"couldn't read source vdisk %s's storage config: %v", sourceVdiskID, err)
	}

	sourceClusterConfig = &sourceStorageConfig.StorageCluster

	// if a targetClusterID is given, check if it's not the same as the source cluster ID,
	// and if it is not, get its config
	if targetClusterID != "" {
		sourceVdiskNBDConfig, err := config.ReadVdiskNBDConfig(configSource, sourceVdiskID)
		if err != nil {
			return fmt.Errorf(
				"couldn't read source vdisk %s's storage config: %v", sourceVdiskID, err)
		}

		if sourceVdiskNBDConfig.StorageClusterID != targetClusterID {
			targetClusterConfig, err = config.ReadStorageClusterConfig(configSource, targetClusterID)
			if err != nil {
				return fmt.Errorf(
					"couldn't read target vdisk %s's storage config: %v", targetVdiskID, err)
			}
		}
	}

	// copy the vdisk
	switch stype := sourceStaticConfig.Type.StorageType(); stype {
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

	VdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")
	VdiskCmd.Flags().BoolVar(
		&vdiskCmdCfg.ForceSameStorageCluster, "same", false,
		"enable flag to force copy within the same nbd servers")
}
