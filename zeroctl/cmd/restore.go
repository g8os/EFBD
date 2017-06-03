package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zeroctl/cmd/restore"
)

// RestoreCmd represents the restore subcommand
var RestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a zero-os resource",
}

func init() {
	RestoreCmd.AddCommand(
		restore.VdiskCmd,
	)
}
