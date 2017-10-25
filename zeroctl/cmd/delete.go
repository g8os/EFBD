package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zeroctl/cmd/delvdisk"
)

// DeleteCmd represents the delete subcommand
var DeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a zero-os resource",
}

func init() {
	DeleteCmd.AddCommand(
		delvdisk.VdiskCmd,
	)
}
