package copyvdisk

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/zero-os/0-Disk/nbd/ardb"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog/copy"
	tlogserver "github.com/zero-os/0-Disk/tlog/tlogserver/server"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCmdCfg struct {
	SourceConfig            config.SourceConfig
	ForceSameStorageCluster bool
	PrivKey                 string
	FlushSize               int
	JobCount                int
	Force                   bool
}

// VdiskCmd represents the vdisk copy subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisk source_vdiskid target_vdiskid",
	Short: "Copy a vdisk",
	RunE:  copyVdisk,
}

func copyVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.InfoLevel
	if cmdconfig.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	// create config source
	configSource, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer configSource.Close()

	log.Debug("parsing positional arguments...")

	// validate pos arg length
	argn := len(args)
	if argn < 2 {
		return errors.New("not enough arguments")
	} else if argn > 2 {
		return errors.New("too many arguments")
	}

	// store pos arguments in named variables
	sourceVdiskID, targetVdiskID := args[0], args[1]

	// TODO: support slave clusters!!!

	// try to read the configs of source vdisk
	srcStaticCfg, err := config.ReadVdiskStaticConfig(configSource, sourceVdiskID)
	if err != nil {
		return err
	}
	srcNBDConfig, err := config.ReadVdiskNBDConfig(configSource, sourceVdiskID)
	if err != nil {
		return err
	}
	srcClusterConfig, err := config.ReadStorageClusterConfig(configSource, srcNBDConfig.StorageClusterID)
	if err != nil {
		return err
	}
	sourceCluster, err := ardb.NewCluster(*srcClusterConfig, nil)
	if err != nil {
		return err
	}

	// try to read the configs of target vdisk
	dstStaticConfig, err := config.ReadVdiskStaticConfig(configSource, targetVdiskID)
	if err != nil {
		return err
	}
	dstNBDConfig, err := config.ReadVdiskNBDConfig(configSource, targetVdiskID)
	if err != nil {
		return err
	}

	var targetCluster ardb.StorageCluster
	// only create target cluster if the source and target cluster IDs are different
	if srcNBDConfig.StorageClusterID != dstNBDConfig.StorageClusterID {
		dstClusterConfig, err := config.ReadStorageClusterConfig(configSource, dstNBDConfig.StorageClusterID)
		if err != nil {
			return err
		}
		targetCluster, err = ardb.NewCluster(*dstClusterConfig, nil)
		if err != nil {
			return err
		}
		err = checkVdiskExists(targetVdiskID, dstStaticConfig.Type, targetCluster)
		if err != nil {
			return err
		}
	} else {
		err = checkVdiskExists(targetVdiskID, dstStaticConfig.Type, sourceCluster)
		if err != nil {
			return err
		}
	}

	// 1. copy the ARDB (meta)data

	sourceConfig := storage.CopyVdiskConfig{
		VdiskID:   sourceVdiskID,
		Type:      srcStaticCfg.Type,
		BlockSize: int64(srcStaticCfg.BlockSize),
	}
	targetConfig := storage.CopyVdiskConfig{
		VdiskID:   targetVdiskID,
		Type:      dstStaticConfig.Type,
		BlockSize: int64(dstStaticConfig.BlockSize),
	}

	err = storage.CopyVdisk(sourceConfig, targetConfig, sourceCluster, targetCluster)
	if err != nil {
		return err
	}

	// 2. copy the tlog data if it is needed

	err = copy.Copy(context.Background(), configSource, copy.Config{
		SourceVdiskID: sourceVdiskID,
		TargetVdiskID: targetVdiskID,
		PrivKey:       vdiskCmdCfg.PrivKey,
		FlushSize:     vdiskCmdCfg.FlushSize,
		JobCount:      vdiskCmdCfg.JobCount,
	})
	if err != nil {
		return fmt.Errorf("failed to copy/generate tlog data for vdisk `%v`: %v", targetVdiskID, err)
	}

	return nil
}

// checkVdiskExists checks if the vdisk in question already/still exists,
// and if so, and the force flag is specified, delete the vdisk.
func checkVdiskExists(id string, t config.VdiskType, cluster ardb.StorageCluster) error {
	// check if vdisk exists
	exists, err := storage.VdiskExists(id, t, cluster)
	if !exists {
		return nil // vdisk doesn't exist, so nothing to do
	}
	if err != nil {
		return fmt.Errorf("couldn't check if vdisk %s already exists: %v", id, err)
	}
	if !vdiskCmdCfg.Force {
		return fmt.Errorf("cannot copy to vdisk %s as it already exists", id)
	}

	// delete vdisk, as it exists and `--force` is specified
	deleted, err := storage.DeleteVdisk(id, t, cluster)
	if err != nil {
		return fmt.Errorf("couldn't delete vdisk %s: %v", id, err)
	}
	if !deleted {
		return fmt.Errorf("couldn't delete vdisk %s for an unknown reason", id)
	}

	// delete 0-Stor (meta)data for this vdisk
	if t.TlogSupport() {
		// TODO: also delete actual tlog meta(data) from 0-Stor cluster for the supported vdisks ?!?!
		//       https://github.com/zero-os/0-Disk/issues/147
	}

	// vdisk did exist, but we were able to delete all the exiting (meta)data
	return nil
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

NOTE: the storage types and block sizes of source and target vdisk
  need to be equal, else an error is returned.
`

	VdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")
	VdiskCmd.Flags().BoolVar(
		&vdiskCmdCfg.ForceSameStorageCluster, "same", false,
		"enable flag to force copy within the same nbd servers")

	VdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.PrivKey,
		"priv-key", "12345678901234567890123456789012",
		"private key")

	VdiskCmd.Flags().IntVarP(
		&vdiskCmdCfg.JobCount,
		"jobs", "j", runtime.NumCPU(),
		"the amount of parallel jobs to run the tlog generator")

	VdiskCmd.Flags().IntVar(
		&vdiskCmdCfg.FlushSize,
		"flush-size", tlogserver.DefaultConfig().FlushSize,
		"number of tlog blocks in one flush")

	VdiskCmd.Flags().BoolVarP(
		&vdiskCmdCfg.Force,
		"force", "f", false,
		"when given, delete the target vdisk if it already existed")
}
