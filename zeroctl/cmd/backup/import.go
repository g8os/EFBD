package backup

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/nbd/nbdserver/tlog"

	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

// ImportVdiskCmd represents the vdisk import subcommand
var ImportVdiskCmd = &cobra.Command{
	Use:   "vdisk vdiskid cryptoKey snapshotID",
	Short: "import a vdisk",
	RunE:  importVdisk,
}

func importVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	// parse the position arguments
	err := parseImportPosArguments(args)
	if err != nil {
		return err
	}

	err = checkVdiskExists(vdiskCmdCfg.VdiskID)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := backup.Config{
		VdiskID:             vdiskCmdCfg.VdiskID,
		SnapshotID:          vdiskCmdCfg.SnapshotID,
		BlockSize:           vdiskCmdCfg.ExportBlockSize,
		BlockStorageConfig:  vdiskCmdCfg.SourceConfig,
		BackupStorageConfig: vdiskCmdCfg.BackupStorageConfig,
		JobCount:            vdiskCmdCfg.JobCount,
		CompressionType:     vdiskCmdCfg.CompressionType,
		CryptoKey:           vdiskCmdCfg.PrivateKey,
		Force:               vdiskCmdCfg.Force,
	}

	return backup.Import(ctx, cfg)
}

// checkVdiskExists checks if the vdisk in question already/still exists,
// and if so, and the force flag is specified, delete the (meta)data.
func checkVdiskExists(vdiskID string) error {
	// create config source
	configSource, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer configSource.Close()

	staticConfig, err := config.ReadVdiskStaticConfig(configSource, vdiskID)
	if err != nil {
		return fmt.Errorf(
			"cannot read static vdisk config for vdisk %s: %v", vdiskID, err)
	}
	nbdStorageConfig, err := config.ReadNBDStorageConfig(configSource, vdiskID)
	if err != nil {
		return fmt.Errorf(
			"cannot read nbd storage config for vdisk %s: %v", vdiskID, err)
	}

	exists, err := storage.VdiskExists(
		vdiskID, staticConfig.Type, &nbdStorageConfig.StorageCluster)
	if !exists {
		return nil // vdisk doesn't exist, so nothing to do
	}
	if err != nil {
		return fmt.Errorf("couldn't check if vdisk %s already exists: %v", vdiskID, err)
	}

	if !vdiskCmdCfg.Force {
		return fmt.Errorf("cannot import vdisk %s as it already exists", vdiskID)
	}

	vdisks := map[string]config.VdiskType{vdiskID: staticConfig.Type}

	// delete metadata
	serverConfig, err := nbdStorageConfig.StorageCluster.FirstAvailableServer()
	if err != nil {
		return err
	}
	err = storage.DeleteMetadata(*serverConfig, vdisks)
	if err != nil {
		return fmt.Errorf(
			"couldn't delete metadata for vdisks from %s@%d: %v",
			serverConfig.Address, serverConfig.Database, err)
	}
	// make this easier
	// see: https://github.com/zero-os/0-Disk/issues/481
	err = deleteTlogMetadata(*serverConfig, vdisks)
	if err != nil {
		return fmt.Errorf(
			"couldn't delete tlog metadata for vdisks from %s@%d: %v",
			serverConfig.Address, serverConfig.Database, err)
	}

	// delete data
	for _, serverConfig := range nbdStorageConfig.StorageCluster.Servers {
		err := storage.DeleteData(serverConfig, vdisks)
		if err != nil {
			return fmt.Errorf(
				"couldn't delete data for vdisk %s from %s@%d: %v",
				vdiskID, serverConfig.Address, serverConfig.Database, err)
		}
	}

	// vdisk did exist, but we were able to delete all the exiting (meta)data
	return nil
}

func deleteTlogMetadata(serverCfg config.StorageServerConfig, vdiskMap map[string]config.VdiskType) error {
	var vdisks []string
	for vdiskID, vdiskType := range vdiskMap {
		if vdiskType.TlogSupport() {
			vdisks = append(vdisks, vdiskID)
		}
	}

	// TODO: ensure that vdisk also have an active tlog configuration,
	//       as this is still optional even though it might support it type-wise.
	// TODO: also delete actual tlog meta(data) from 0-Stor cluster for the supported vdisks
	//       https://github.com/zero-os/0-Disk/issues/147

	return tlog.DeleteMetadata(serverCfg, vdisks...)
}

func parseImportPosArguments(args []string) error {
	// validate pos arg length
	argn := len(args)
	if argn < 3 {
		return errors.New("not enough arguments")
	} else if argn > 3 {
		return errors.New("too many arguments")
	}

	vdiskCmdCfg.VdiskID = args[0]
	vdiskCmdCfg.SnapshotID = args[2]

	return vdiskCmdCfg.PrivateKey.Set(args[1])
}

func init() {
	ImportVdiskCmd.Long = ImportVdiskCmd.Short + `

Remember to use the same (snapshot) name,
crypto (private) key and the compression type,
as you used while exporting the backup in question.

  If an error occured during the import process,
blocks might already have been written to the block storage.
These blocks won't be deleted in case of an error,
so note that you might end up with some "garbage" in such a scenario.
Deleting the vdisk in such a scenario will help with this problem.

  The FTP information is given as the --storage flag,
here are some examples of valid values for that flag:
	+ localhost:22
	+ ftp://1.2.3.4:200
	+ ftp://user@127.0.0.1:200
	+ ftp://user:pass@12.30.120.200:3000

Alternatively you can also give a local directory path to the --storage flag,
to backup to the local file system instead.
This is also the default in case the --storage flag is not specified.
`

	ImportVdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")

	ImportVdiskCmd.Flags().Int64VarP(
		&vdiskCmdCfg.ExportBlockSize, "blocksize", "b", backup.DefaultBlockSize,
		"the size of the exported (deduped) blocks")
	ImportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.CompressionType, "compression", "c",
		"the compression type to use, options { lz4, xz }")
	ImportVdiskCmd.Flags().IntVarP(
		&vdiskCmdCfg.JobCount, "jobs", "j", runtime.NumCPU(),
		"the amount of parallel jobs to run")

	ImportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.BackupStorageConfig, "storage", "s",
		"ftp server url or local dir path to import the backup from")

	ImportVdiskCmd.Flags().BoolVarP(
		&vdiskCmdCfg.Force,
		"force", "f", false,
		"when given, delete the vdisk if it already existed")
}
