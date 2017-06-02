package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zerodisk/cmd/restore"
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
