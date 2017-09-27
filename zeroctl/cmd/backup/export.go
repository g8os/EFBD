package backup

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"

	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

// ExportVdiskCmd represents the vdisk export subcommand
var ExportVdiskCmd = &cobra.Command{
	Use:   "vdisk vdiskid cryptoKey [snapshotID]",
	Short: "export a vdisk",
	RunE:  exportVdisk,
}

func exportVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	// parse the position arguments
	err := parseExportPosArguments(args)
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

	err = backup.Export(ctx, cfg)
	if err != nil {
		return err
	}

	fmt.Println(vdiskCmdCfg.SnapshotID)
	return nil
}

func parseExportPosArguments(args []string) error {
	// validate pos arg length
	argn := len(args)
	if argn < 2 {
		return errors.New("not enough arguments")
	} else if argn > 3 {
		return errors.New("too many arguments")
	}

	vdiskCmdCfg.VdiskID = args[0]
	if argn == 3 {
		vdiskCmdCfg.SnapshotID = args[2]
	} else {
		epoch := time.Now().UTC().Unix()
		vdiskCmdCfg.SnapshotID = fmt.Sprintf("%s_%d", vdiskCmdCfg.VdiskID, epoch)
	}

	return vdiskCmdCfg.PrivateKey.Set(args[1])
}

func init() {
	ExportVdiskCmd.Long = ExportVdiskCmd.Short + `

Remember to keep note of the used (snapshot) name,
crypto (private) key and the compression type,
as you will need the same information when importing the exported backup.

The crypto (private) key has a required fixed length of 32 bytes,
and cannot be all zeroes.

  If an error occured during the export process,
deduped blocks might already have been written to the FTP server.
These blocks won't be deleted in case of an error,
so note that you might end up with some "garbage" in such a scenario.

  If the snapshotID is not given,
one will be generated automatically using the "<vdiskID>_epoch" format.
The used snapshotID will be printed in the STDOUT in case
no (fatal) error occured, at the end of the command's lifetime.

  The FTP information is given using the --storage flag,
here are some examples of valid values for that flag:
	+ localhost:22
	+ ftp://1.2.3.4:200
	+ ftp://user@127.0.0.1:200
	+ ftp://user:pass@12.30.120.200:3000

Alternatively you can also give a local directory path to the --storage flag,
to backup to the local file system instead.
This is also the default in case the --storage flag is not specified.

  When the --force flag is given,
a deduped map will be overwritten if it already existed,
AND if it couldn't be loaded, due to being corrupt or encrypted/compressed,
using a different private key or compression type, than the one(s) used right now.
`

	ExportVdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")

	ExportVdiskCmd.Flags().Int64VarP(
		&vdiskCmdCfg.ExportBlockSize, "blocksize", "b", backup.DefaultBlockSize,
		"the size of the exported (deduped) blocks")
	ExportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.CompressionType, "compression", "c",
		"the compression type to use, options { lz4, xz }")
	ExportVdiskCmd.Flags().IntVarP(
		&vdiskCmdCfg.JobCount, "jobs", "j", runtime.NumCPU(),
		"the amount of parallel jobs to run")

	ExportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.BackupStorageConfig, "storage", "s",
		"ftp server url or local dir path to export the backup to")

	ExportVdiskCmd.Flags().BoolVarP(
		&vdiskCmdCfg.Force,
		"force", "f", false,
		"when given, overwrite a deduped map if it can't be loaded")
}
