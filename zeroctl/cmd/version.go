package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk"
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

// outputVersion prints to the STDOUT,
// the tool version, runtime info, and optionally the commit hash.
func outputVersion(*cobra.Command, []string) {
	// Tool Version
	version := "Version: " + zerodisk.CurrentVersion.String()

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
