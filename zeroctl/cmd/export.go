package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zeroctl/cmd/backup"
)

// ExportCmd represents the export subcommand
var ExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export a zero-os resource",
}

func init() {
	ExportCmd.AddCommand(
		backup.ExportVdiskCmd,
	)
}
