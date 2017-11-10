package backup

import (
	"context"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/backup"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/copy"
	tlogserver "github.com/zero-os/0-Disk/tlog/tlogserver/server"

	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

// ImportVdiskCmd represents the vdisk import subcommand
var ImportVdiskCmd = &cobra.Command{
	Use:   "vdisk vdiskid snapshotID",
	Short: "import a vdisk",
	RunE:  importVdisk,
}

// import only configuration
// see `init` for more information
// about the meaning of each config property.
var importVdiskCmdCfg struct {
	TlogPrivKey string
	FlushSize   int
}

func importVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.InfoLevel
	if cmdconfig.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	// parse the position arguments
	err := parseImportPosArguments(args)
	if err != nil {
		return err
	}

	source, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	defer source.Close()
	configSource := config.NewOnceSource(source)

	err = checkVdiskExists(vdiskCmdCfg.VdiskID, configSource)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := backup.Config{
		VdiskID:                  vdiskCmdCfg.VdiskID,
		SnapshotID:               vdiskCmdCfg.SnapshotID,
		BackupStoragDriverConfig: createBackupStorageConfigFromFlags(),
		JobCount:                 vdiskCmdCfg.JobCount,
		CompressionType:          vdiskCmdCfg.CompressionType,
		CryptoKey:                vdiskCmdCfg.PrivateKey,
		Force:                    vdiskCmdCfg.Force,
		ConfigSource:             source,
	}

	log.Info("Importing the vdisk")
	err = backup.Import(ctx, cfg)
	if err != nil {
		return err
	}

	// check if this vdisk has tlog cluster
	hasTlogCluster, err := tlog.HasTlogCluster(configSource, vdiskCmdCfg.VdiskID)
	if err != nil || !hasTlogCluster {
		return err
	}

	log.Infof("generate tlog data")

	vdiskNbdConf, err := config.ReadVdiskNBDConfig(configSource, vdiskCmdCfg.VdiskID)
	if err != nil {
		log.Errorf("failed to read vdisk nbd config of `%v`: %v", vdiskCmdCfg.VdiskID, err)
		return err
	}

	clusterConf, err := config.ReadStorageClusterConfig(configSource, vdiskNbdConf.StorageClusterID)
	if err != nil {
		log.Errorf("failed to read storage cluster config of `%v`: %v", vdiskCmdCfg.VdiskID, err)
		return err
	}

	generator, err := copy.NewGenerator(configSource, copy.Config{
		SourceVdiskID: vdiskCmdCfg.VdiskID,
		TargetVdiskID: vdiskCmdCfg.VdiskID,
		FlushSize:     importVdiskCmdCfg.FlushSize,
		PrivKey:       importVdiskCmdCfg.TlogPrivKey,
		JobCount:      vdiskCmdCfg.JobCount,
	})
	if err != nil {
		return err
	}

	var tlogMetadata storage.TlogMetadata
	tlogMetadata.LastFlushedSequence, err = generator.GenerateFromStorage(ctx)
	if err != nil {
		return err
	}

	// store nbd's tlog metadata
	cluster, err := ardb.NewCluster(*clusterConf, nil)
	if err != nil {
		return err
	}
	return storage.StoreTlogMetadata(vdiskCmdCfg.VdiskID, cluster, tlogMetadata)
}

// checkVdiskExists checks if the vdisk in question already/still exists,
// and if so, and the force flag is specified, delete the vdisk.
func checkVdiskExists(vdiskID string, configSource config.Source) error {
	// check if vdisk exists
	exists, vdiskType, cluster, err := storage.VdiskExists(vdiskID, configSource)
	if err != nil {
		return errors.Wrapf(err, "couldn't check if vdisk %s already exists", vdiskID)
	}
	if !exists {
		return nil // vdisk doesn't exist, so nothing to do
	}
	if !vdiskCmdCfg.Force {
		return errors.Newf("cannot restore vdisk %s as it already exists", vdiskID)
	}

	// delete vdisk, as it exists and `--force` is specified
	deleted, err := storage.DeleteVdiskInCluster(vdiskID, vdiskType, cluster)
	if err != nil {
		return errors.Wrapf(err, "couldn't delete vdisk %s", vdiskID)
	}
	if !deleted {
		return errors.Newf("couldn't delete vdisk %s for an unknown reason", vdiskID)
	}

	// delete 0-Stor (meta)data for this vdisk
	if vdiskType.TlogSupport() {
		// TODO: also delete actual tlog meta(data) from 0-Stor cluster for the supported vdisks ?!?!
		//       https://github.com/zero-os/0-Disk/issues/147
	}

	// vdisk did exist, but we were able to delete all the exiting (meta)data
	return nil
}

func parseImportPosArguments(args []string) error {
	// validate pos arg length
	argn := len(args)
	if argn < 2 {
		return errors.New("not enough arguments")
	} else if argn > 2 {
		return errors.New("too many arguments")
	}

	vdiskCmdCfg.VdiskID = args[0]
	vdiskCmdCfg.SnapshotID = args[1]

	return nil
}

func init() {
	ImportVdiskCmd.Long = ImportVdiskCmd.Short + `

Remember to use the same (snapshot) name,
crypto (private) key and the compression type,
as you used while exporting the backup in question.

The crypto (private) key has a required fixed length of 32 bytes.
If the snapshot wasn't encrypted, no key should be given,
giving a key in this scenario will fail the import.

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


  When the --storage flag contains an FTP storage config and at least one of 
--tls-server/--tls-cert/--tls-insecure/--tls-ca flags are given, 
FTPS (FTP over SSL) is used instead of a plain FTP connection. 
This enables importing backups in a private and secure fashion,
discouraging eavesdropping, tampering, and message forgery.
When the configured server does not support FTPS an error will be returned.
`

	ImportVdiskCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")

	ImportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.CompressionType, "compression", "c",
		"the compression type to use, options { lz4, xz }")
	ImportVdiskCmd.Flags().VarP(
		&vdiskCmdCfg.PrivateKey, "key", "k",
		"an optional 32 byte fixed-size private key used for decryption when given")
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

	ImportVdiskCmd.Flags().StringVar(
		&importVdiskCmdCfg.TlogPrivKey,
		"tlog-priv-key", "12345678901234567890123456789012",
		"tlog private key")

	ImportVdiskCmd.Flags().IntVar(
		&importVdiskCmdCfg.FlushSize,
		"flush-size", tlogserver.DefaultConfig().FlushSize,
		"number of tlog blocks in one flush")

	ImportVdiskCmd.Flags().BoolVar(
		&vdiskCmdCfg.TLSConfig.InsecureSkipVerify,
		"tls-insecure", false,
		"when given FTP over SSL will be used without cert verification")
	ImportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.ServerName,
		"tls-server", "",
		"certs will be verified when given (required when --tls-insecure is not used)")
	ImportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.CertFile,
		"tls-cert", "",
		"PEM-encoded file containing the TLS Client cert (FTPS will be used when given)")
	ImportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.KeyFile,
		"tls-key", "",
		"PEM-encoded file containing the private TLS client key")
	ImportVdiskCmd.Flags().StringVar(
		&vdiskCmdCfg.TLSConfig.CAFile,
		"tls-ca", "",
		"optional PEM-encoded file containing the TLS CA Pool (defaults to system pool when not given)")
}
