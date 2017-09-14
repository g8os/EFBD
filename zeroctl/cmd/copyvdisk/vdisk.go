package copyvdisk

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog/generator"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCmdCfg struct {
	SourceConfig            config.SourceConfig
	ForceSameStorageCluster bool
	DataShards              int
	ParityShards            int
	PrivKey                 string
	JobCount                int
}

// VdiskCmd represents the vdisk copy subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisk source_vdiskid target_vdiskid [target_clusterid]",
	Short: "Copy a vdisk",
	RunE:  copyVdisk,
}

func copyVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.InfoLevel
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

	// try to read the Vdisk config of target vdisk
	targetStaticConfig, err := config.ReadVdiskStaticConfig(configSource, targetVdiskID)
	if err != nil {
		return fmt.Errorf(
			"couldn't read source vdisk %s's static config: %v", targetVdiskID, err)
	}

	if targetStaticConfig.Type.TlogSupport() {
		log.Infof("generating tlog data for target vdisk `%v`", targetVdiskID)
		generator, err := generator.New(configSource, generator.Config{
			SourceVdiskID: sourceVdiskID,
			TargetVdiskID: targetVdiskID,
			DataShards:    vdiskCmdCfg.DataShards,
			ParityShards:  vdiskCmdCfg.ParityShards,
			PrivKey:       vdiskCmdCfg.PrivKey,
			JobCount:      vdiskCmdCfg.JobCount,
		})
		if err != nil {
			return fmt.Errorf("failed to create tlog generator: %v", err)
		}

		if err := generator.GenerateFromStorage(context.Background()); err != nil {
			return fmt.Errorf("failed to generate tlog data for vdisk `%v` : %v", targetVdiskID, err)
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

	VdiskCmd.Flags().IntVar(
		&vdiskCmdCfg.DataShards,
		"data-shards", 4,
		"data shards (K) variable of erasure encoding")
	VdiskCmd.Flags().IntVar(
		&vdiskCmdCfg.ParityShards,
		"parity-shards", 2,
		"parity shards (M) variable of erasure encoding")
	VdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.PrivKey,
		"priv-key", "12345678901234567890123456789012",
		"private key")

	VdiskCmd.Flags().IntVarP(
		&vdiskCmdCfg.JobCount,
		"jobs", "j", runtime.NumCPU(),
		"the amount of parallel jobs to run the tlog generator")

}
