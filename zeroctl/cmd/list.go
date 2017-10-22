package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zeroctl/cmd/list"
)

// ListCmd represents the list subcommand
var ListCmd = &cobra.Command{
	Use:   "list",
	Short: "List zero-os resources",
}

func init() {
	ListCmd.AddCommand(
		list.VdisksCmd,
	)
}
