package cmd

import (
	"github.com/g8os/blockstor/g8stor/cmd/restore"
	"github.com/spf13/cobra"
)

// RestoreCmd represents the restore subcommand
var RestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a g8os resource",
}

func init() {
	RestoreCmd.AddCommand(
		restore.VdiskCmd,
	)
}
