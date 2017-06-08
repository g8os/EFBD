package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zeroctl/cmd/copyvdisk"
)

// CopyCmd represents the copy subcommand
var CopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy a zero-os resource",
}

func init() {
	CopyCmd.AddCommand(
		copyvdisk.VdiskCmd,
	)
}
