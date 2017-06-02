package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zerodisk/cmd/copyvdisk"
)

// CopyCmd represents the copy subcommand
var CopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy a g8os resource",
}

func init() {
	CopyCmd.AddCommand(
		copyvdisk.DedupedCmd,
		copyvdisk.NondedupedCmd,
		copyvdisk.VdiskCmd,
	)
}
