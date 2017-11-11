package backup

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"

	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

// ExportVdiskCmd represents the vdisk export subcommand
var ExportVdiskCmd = &cobra.Command{
	Use:   "vdisk vdiskid [snapshotID]",
	Short: "export a vdisk",
	RunE:  exportVdisk,
}

// export only configuration
// see `init` for more information
// about the meaning of each config property.
var exportVdiskCmdCfg struct {
	ExportBlockSize int64
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

	// create config source
	cs, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer cs.Close()
	configSource := config.NewOnceSource(cs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := backup.Config{
		VdiskID:                  vdiskCmdCfg.VdiskID,
		SnapshotID:               vdiskCmdCfg.SnapshotID,
		BlockSize:                exportVdiskCmdCfg.ExportBlockSize,
		BackupStoragDriverConfig: createBackupStorageConfigFromFlags(),
		JobCount:                 vdiskCmdCfg.JobCount,
		CompressionType:          vdiskCmdCfg.CompressionType,
		CryptoKey:                vdiskCmdCfg.PrivateKey,
		Force:                    vdiskCmdCfg.Force,
		ConfigSource:             configSource,
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
	if argn < 1 {
		return errors.New("not enough arguments")
	} else if argn > 2 {
		return errors.New("too many arguments")
	}

	vdiskCmdCfg.VdiskID = args[0]
	if argn == 2 {
		vdiskCmdCfg.SnapshotID = args[1]
	} else {
		epoch := time.Now().UTC().Unix()
		vdiskCmdCfg.SnapshotID = fmt.Sprintf("%s_%d", vdiskCmdCfg.VdiskID, epoch)
	}

	return nil
}

func init() {
	ExportVdiskCmd.Long = ExportVdiskCmd.Short + `

Remember to keep note of the used (snapshot) name,
crypto (private) key and the compression type,
as you will need the same information when importing the exported backup.

The crypto (private) key has a required fixed length of 32 bytes.
If no key is given, the compressed snapshot will not be encrypted.

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

  When the --storage flag contains an FTP storage config and at least one of 
--tls-server/--tls-cert/--tls-insecure/--tls-ca flags are given, 
FTPS (FTP over SSL) is used instead of a plain FTP connection. 
This enables exporting backups in a private and secure fashion,
discouraging eavesdropping, tampering, and message forgery.
When the configured server does not support FTPS an error will be returned.
`

	ExportVdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")

	ExportVdiskCmd.Flags().Int64VarP(
		&exportVdiskCmdCfg.ExportBlockSize, "blocksize", "b", backup.DefaultBlockSize,
		"the size of the exported (deduped) blocks")
	ExportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.CompressionType, "compression", "c",
		"the compression type to use, options { lz4, xz }")
	ExportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.PrivateKey, "key", "k",
		"an optional 32 byte fixed-size private key used for encryption when given")
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

	ExportVdiskCmd.Flags().BoolVar(
		&vdiskCmdCfg.TLSConfig.InsecureSkipVerify,
		"tls-insecure", false,
		"when given FTP over SSL will be used without cert verification")
	ExportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.ServerName,
		"tls-server", "",
		"certs will be verified when given (required when --tls-insecure is not used)")
	ExportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.CertFile,
		"tls-cert", "",
		"PEM-encoded file containing the TLS Client cert (FTPS will be used when given)")
	ExportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.KeyFile,
		"tls-key", "",
		"PEM-encoded file containing the private TLS client key")
	ExportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.CAFile,
		"tls-ca", "",
		"optional PEM-encoded file containing the TLS CA Pool (defaults to system pool when not given)")
}
