package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zerodisk/cmd/list"
)

// ListCmd represents the list subcommand
var ListCmd = &cobra.Command{
	Use:   "list",
	Short: "List g8os resources",
}

func init() {
	ListCmd.AddCommand(
		list.VdiskCmd,
	)
}
