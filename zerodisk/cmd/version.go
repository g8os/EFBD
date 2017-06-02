package cmd

import (
	"fmt"
	"runtime"

	"github.com/zero-os/0-Disk"
	"github.com/spf13/cobra"
)

var (
	// CommitHash represents the Git commit hash at built time
	CommitHash string
	// BuildDate represents the date when this tool suite was built
	BuildDate string
)

// VersionCmd represents the version subcommand
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Output the version information",
	Long:  "Outputs the tool version, runtime information, and optionally the commit hash.",
	Run:   outputVersion,
}

func outputVersion(*cobra.Command, []string) {
	// Tool Version
	version := "Version: " + blockstor.CurrentVersion.String()

	// Build (Git) Commit Hash
	if CommitHash != "" {
		version += "\r\nBuild: " + CommitHash
		if BuildDate != "" {
			version += " " + BuildDate
		}
	}

	// Output version and runtime information
	fmt.Printf("%s\r\nRuntime: %s %s\r\n",
		version,
		runtime.Version(), // Go Version
		runtime.GOOS,      // OS Name
	)
}
