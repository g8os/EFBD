package list

import (
	"errors"
	"fmt"

	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"

	"github.com/spf13/cobra"
	zerodiskcfg "github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdisksCmdCfg struct {
	SourceConfig zerodiskcfg.SourceConfig
}

// VdisksCmd represents the list vdisk subcommand
var VdisksCmd = &cobra.Command{
	Use:   "vdisks clusterID",
	Short: "List all vdisks available on a cluster",
	RunE:  listVdisks,
}

func listVdisks(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if config.Verbose {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	// create config source
	source, err := zerodiskcfg.NewSource(vdisksCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer source.Close()

	// get command line argument
	argn := len(args)
	if argn < 1 {
		return errors.New("no cluster identifier given")
	}
	if argn > 1 {
		return errors.New("too many cluster identifiers given")
	}
	clusterID := args[0]

	// read cluster config
	clusterConfig, err := zerodiskcfg.ReadStorageClusterConfig(source, clusterID)
	if err != nil {
		return err
	}

	// create cluster
	cluster, err := ardb.NewCluster(*clusterConfig, nil)
	if err != nil {
		return err
	}

	// list vdisks
	vdiskIDs, err := storage.ListVdisks(cluster)
	if err != nil {
		return err
	}
	if len(vdiskIDs) == 0 {
		return errors.New("no vdisks could be found in cluster " + clusterID)
	}

	// print at least 1 vdisk fond from the specified storage
	for _, vdiskID := range vdiskIDs {
		fmt.Println(vdiskID)
	}
	return nil
}

func init() {
	VdisksCmd.Long = VdisksCmd.Short + `

WARNING: This command is very slow, and might take a while to finish!
  It might also decrease the performance of the ardb server
  in question, by locking the server down for each operation.
`

	VdisksCmd.Flags().Var(
		&vdisksCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")
}
