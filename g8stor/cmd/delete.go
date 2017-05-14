package cmd

import (
	"github.com/g8os/blockstor/g8stor/cmd/delvdisk"
	"github.com/spf13/cobra"
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
	)
}
