package zerodisk

import (
	"bytes"
	"fmt"
	"runtime"

	"github.com/zero-os/0-Disk/log"
)

var (
	// CurrentVersion represents the current global
	// version of the zerodisk modules
	CurrentVersion = NewVersion(1, 1, 0, versionLabel("beta-1"))
	// CommitHash represents the Git commit hash at built time
	CommitHash string
	// BuildDate represents the date when this tool suite was built
	BuildDate string
)

// PrintVersion prints the current version
func PrintVersion() {
	version := "Version: " + CurrentVersion.String()

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

// LogVersion prints the version at log level info
// meant to log the version at startup of a server
func LogVersion() {
	// log version
	log.Info("Version: " + CurrentVersion.String())

	// log build (Git) Commit Hash
	if CommitHash != "" {
		build := "Build: " + CommitHash
		if BuildDate != "" {
			build += " " + BuildDate
		}

		log.Info(build)
	}
}

// VersionFromUInt32 creates a version from a given uint32 number.
func VersionFromUInt32(v uint32) Version {
	return Version{
		Number: VersionNumber(v),
		Label:  nil,
	}
}

// NewVersion creates a new version
func NewVersion(major, minor, patch uint8, label *VersionLabel) Version {
	number := (VersionNumber(major) << 16) |
		(VersionNumber(minor) << 8) |
		VersionNumber(patch)
	return Version{
		Number: number,
		Label:  label,
	}
}

type (
	// Version defines the version version information,
	// used by zeroctl services.
	Version struct {
		Number VersionNumber
		Label  *VersionLabel
	}

	// VersionNumber defines the semantic version number,
	// used by zeroctl services.
	VersionNumber uint32

	// VersionLabel defines an optional version extension,
	// used by zeroctl services.
	VersionLabel [8]byte
)

// Compare returns an integer comparing this version
// with another version. { lt=-1 ; eq=0 ; gt=1 }
func (v Version) Compare(other Version) int {
	// are the actual versions not equal?
	if v.Number < other.Number {
		return -1
	} else if v.Number > other.Number {
		return 1
	}

	// concidered to be equal versions
	return 0
}

// UInt32 returns the integral version
// of this Version.
func (v Version) UInt32() uint32 {
	return uint32(v.Number)
}

// String returns the string version
// of this Version.
func (v Version) String() string {
	str := fmt.Sprintf("%d.%d.%d",
		(v.Number>>16)&0xFF, // major
		(v.Number>>8)&0xFF,  // minor
		v.Number&0xFF,       // patch
	)

	if v.Label == nil {
		return str
	}

	label := bytes.Trim(v.Label[:], "\x00")
	return str + "-" + string(label)
}

func versionLabel(str string) *VersionLabel {
	var label VersionLabel
	copy(label[:], str[:])
	return &label
}
