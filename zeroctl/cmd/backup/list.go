package backup

import (
	"fmt"
	"regexp"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var listSnapshotsCmdCfg struct {
	NameRegexp string
}

// ListSnapshotsCmd represents the list-snapshot subcommand
var ListSnapshotsCmd = &cobra.Command{
	Use:   "snapshots",
	Short: "list all snapshots available",
	RunE:  listSnapshots,
}

func listSnapshots(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	// create optional Regexp-based predicate
	var pred func(snapshotID string) bool
	if listSnapshotsCmdCfg.NameRegexp != "" {
		regexp, err := regexp.Compile(listSnapshotsCmdCfg.NameRegexp)
		if err != nil {
			return errors.Wrap(err, "invalid regexp given for `--name`")
		}
		pred = regexp.MatchString
	}

	// create backup storage config based on our flags
	backupStoragDriverConfig := createBackupStorageConfigFromFlags()

	// list and filter all snapshots on the given backup storage
	snapshotIDs, err := backup.ListSnapshots(backupStoragDriverConfig, pred)
	if err != nil {
		return err
	}
	if len(snapshotIDs) == 0 {
		if pred != nil {
			return errors.Newf(
				"no snapshots could be find at %s whose identifier match `%s`",
				vdiskCmdCfg.BackupStorageConfig.String(), listSnapshotsCmdCfg.NameRegexp)
		}

		return errors.New("no snapshots could be found at " + vdiskCmdCfg.BackupStorageConfig.String())
	}

	// print at least 1 snapshot found from the specified storage
	for _, snapshotID := range snapshotIDs {
		fmt.Println(snapshotID)
	}
	return nil
}

func init() {
	ListSnapshotsCmd.Long = `list all snapshots available locally or an FTP(S) server

For each vdisk that is exported to the backup storage,
an identifier will be printed on a newline.
It identifies the snapshot and is same one as choosen at export time.

  The FTP information is given using the --storage flag,
here are some examples of valid values for that flag:
	+ localhost:22
	+ ftp://1.2.3.4:200
	+ ftp://user@127.0.0.1:200
	+ ftp://user:pass@12.30.120.200:3000

Alternatively you can also give a local directory path to the --storage flag,
to backup to the local file system instead.
This is also the default in case the --storage flag is not specified.

  When the --storage flag contains an FTP storage config and at least one of 
--tls-server/--tls-cert/--tls-insecure/--tls-ca flags are given, 
FTPS (FTP over SSL) is used instead of a plain FTP connection. 
This enables listing backups in a private and secure fashion,
discouraging eavesdropping, tampering, and message forgery.
When the configured server does not support FTPS an error will be returned.
`

	ListSnapshotsCmd.Flags().VarP(
		&vdiskCmdCfg.BackupStorageConfig, "storage", "s",
		"ftp server url or local dir path to list the snapshots from")

	ListSnapshotsCmd.Flags().StringVar(
		&listSnapshotsCmdCfg.NameRegexp, "name", "",
		"list only snapshots which match the given name (supports regexp)")

	ListSnapshotsCmd.Flags().BoolVar(
		&vdiskCmdCfg.TLSConfig.InsecureSkipVerify,
		"tls-insecure", false,
		"when given FTP over SSL will be used without cert verification")
	ListSnapshotsCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.ServerName,
		"tls-server", "",
		"certs will be verified when given (required when --tls-insecure is not used)")
	ListSnapshotsCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.CertFile,
		"tls-cert", "",
		"PEM-encoded file containing the TLS Client cert (FTPS will be used when given)")
	ListSnapshotsCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.KeyFile,
		"tls-key", "",
		"PEM-encoded file containing the private TLS client key")
	ListSnapshotsCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.CAFile,
		"tls-ca", "",
		"optional PEM-encoded file containing the TLS CA Pool (defaults to system pool when not given)")
}
