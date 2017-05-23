package cmd

import (
	"github.com/g8os/blockstor/g8stor/cmd/list"
	"github.com/spf13/cobra"
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
