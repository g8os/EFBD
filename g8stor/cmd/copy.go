package cmd

import (
	"github.com/g8os/blockstor/g8stor/cmd/copyvdisk"
	"github.com/spf13/cobra"
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
	)
}
