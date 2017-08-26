package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zeroctl/cmd/backup"
)

// ImportCmd represents the export subcommand
var ImportCmd = &cobra.Command{
	Use:   "import",
	Short: "Import a zero-os resource",
}

func init() {
	ImportCmd.AddCommand(
		backup.ImportVdiskCmd,
	)
}
