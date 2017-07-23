package list

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	zerodiskcfg "github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCfg struct {
	DatabaseIndex int
}

// VdiskCmd represents the list vdisk subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisks storage_url",
	Short: "List all vdisks available on the url's ardb",
	RunE:  listVdisks,
}

func listVdisks(cmd *cobra.Command, args []string) error {
	argn := len(args)

	if argn < 1 {
		return errors.New("not enough arguments")
	}
	if argn > 1 {
		return errors.New("too many arguments")
	}

	storageURL := args[0]

	logLevel := log.ErrorLevel
	if config.Verbose {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	// scan the specified storage for available vdisks
	vdiskIDs, err := storage.ScanForAvailableVdisks(zerodiskcfg.StorageServerConfig{
		Address:  storageURL,
		Database: vdiskCfg.DatabaseIndex,
	})
	if err != nil {
		return err
	}

	// print at least 1 vdisk fond from the specified storage
	for _, vdiskID := range vdiskIDs {
		fmt.Println(vdiskID)
	}

	// all good
	return nil
}

func init() {
	VdiskCmd.Long = VdiskCmd.Short + `

WARNING: This command is very slow, and might take a while to finish!
  It might also decrease the performance of the ardb server
  in question, by locking the server down for each operation.
`

	VdiskCmd.Flags().IntVar(
		&vdiskCfg.DatabaseIndex,
		"db", 0,
		"database index to use for the ardb connection (0 by default)")
}
