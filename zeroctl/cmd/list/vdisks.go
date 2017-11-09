package list

import (
	"fmt"
	"regexp"

	"github.com/spf13/cobra"
	zerodiskcfg "github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdisksCmdCfg struct {
	SourceConfig zerodiskcfg.SourceConfig
	NameRegexp   string
}

// VdisksCmd represents the list vdisk subcommand
var VdisksCmd = &cobra.Command{
	Use:   "vdisks (clusterID|address[@db])",
	Short: "List all vdisks available on a cluster",
	RunE:  listVdisks,
}

func listVdisks(cmd *cobra.Command, args []string) error {
	logLevel := log.InfoLevel
	if config.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	// get command line argument
	argn := len(args)
	if argn < 1 {
		return errors.New("no cluster identifier given")
	}
	if argn > 1 {
		return errors.New("too many cluster identifiers given")
	}

	// create (uni)cluster
	cluster, err := createCluster(args[0])
	if err != nil {
		return err
	}

	// create optional Regexp-based predicate
	var pred func(vdiskID string) bool
	if vdisksCmdCfg.NameRegexp != "" {
		regexp, err := regexp.Compile(vdisksCmdCfg.NameRegexp)
		if err != nil {
			return errors.Wrap(err, "invalid regexp given for `--name`")
		}
		pred = regexp.MatchString
	}

	// list vdisks
	vdiskIDs, err := storage.ListVdisks(cluster, pred)
	if err != nil {
		return err
	}
	// When no vdisks could be found, log this as info to the user,
	// and return without printing anything.
	// Not finding a vdisk is not concidered an error.
	if len(vdiskIDs) == 0 {
		if pred != nil {
			log.Infof(
				"no vdisks could be find in %s whose identifier match `%s`",
				args[0], vdisksCmdCfg.NameRegexp)
			return nil
		}

		log.Infof("no vdisks could be found in %s", args[0])
		return nil
	}

	// print at least 1 vdisk found from the specified storage
	for _, vdiskID := range vdiskIDs {
		fmt.Println(vdiskID)
	}
	return nil
}

// create a cluster based on the given string,
// which is either a serverConfigStirng or the ID of a pre-configured cluster.
func createCluster(str string) (ardb.StorageCluster, error) {
	serverCfg, err := zerodiskcfg.ParseStorageServerConfigString(str)
	if err == nil {
		return ardb.NewUniCluster(serverCfg, nil)
	}
	log.Debugf("failed to create serverConfig using posarg '%s': %v", str, err)

	// create config source
	source, err := zerodiskcfg.NewSource(vdisksCmdCfg.SourceConfig)
	if err != nil {
		return nil, err
	}
	defer source.Close()

	// read cluster config
	clusterConfig, err := zerodiskcfg.ReadStorageClusterConfig(source, str)
	if err != nil {
		return nil, err
	}

	// create cluster
	return ardb.NewCluster(*clusterConfig, nil)
}

func init() {
	VdisksCmd.Long = VdisksCmd.Short + `

This command can list vdisks on an entire cluster,
as wel as on a single storage server. Some examples:

  	zeroctl vdisk list myCluster
  	zeroctl vdisk list localhost:2000
  	zeroctl vdisk list 127.0.0.1:16379@5

WARNING: This command is very slow, and might take a while to finish!
  It might also decrease the performance of the ardb server
  in question, by locking the server down for each operation.
`

	VdisksCmd.Flags().Var(
		&vdisksCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")

	VdisksCmd.Flags().StringVar(
		&vdisksCmdCfg.NameRegexp, "name", "",
		"list only vdisks which match the given name (supports regexp)")
}
