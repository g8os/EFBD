package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zerodisk/cmd/delvdisk"
)

// DeleteCmd represents the delete subcommand
var DeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a g8os resource",
}

func init() {
	DeleteCmd.AddCommand(
		delvdisk.DedupedCmd,
		delvdisk.NondedupedCmd,
		delvdisk.VdisksCmd,
	)
}
