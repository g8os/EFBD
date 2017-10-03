package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/zeroctl/cmd/backup"
)

// DescribeCmd represents the describe subcommand
var DescribeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describe a zero-os resource",
}

func init() {
	DescribeCmd.AddCommand(
		backup.DescribeSnapshotCmd,
	)
}
