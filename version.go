package zerodisk

import (
	"bytes"
	"fmt"
	"runtime"

	"regexp"
	"strconv"

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

	//version parsing regex
	verRegex = regexp.MustCompile(`^([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5]).([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5]).([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])(?:-([A-Za-z0-9\-]{1,8}))?$`)

	//default version when VersionFromString method is called with empty
	//string (1.1.0)
	defaultVersion = NewVersion(1, 1, 0, nil)
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

// Major returns the Major version of this version number.
func (n VersionNumber) Major() uint8 {
	return uint8(n >> 16)
}

// Minor returns the Minor version of this version number.
func (n VersionNumber) Minor() uint8 {
	return uint8(n >> 8)
}

// Patch returns the Patch version of this version number.
func (n VersionNumber) Patch() uint8 {
	return uint8(n)
}

// String returns the string version
// of this VersionLabel.
func (l *VersionLabel) String() string {
	return string(bytes.Trim(l[:], "\x00"))
}

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
		v.Number.Major(), v.Number.Minor(), v.Number.Patch())
	if v.Label == nil {
		return str
	}

	return str + "-" + v.Label.String()
}

//VersionFromString returns a Version object from the string
//representation
func VersionFromString(ver string) (Version, error) {
	if ver == "" {
		return defaultVersion, nil
	}

	match := verRegex.FindStringSubmatch(ver)
	if len(match) == 0 {
		return Version{}, fmt.Errorf("not a valid version format '%s'", ver)
	}
	num := make([]uint8, 3)
	for i, n := range match[1:4] {
		v, _ := strconv.ParseUint(n, 10, 8)
		num[i] = uint8(v)
	}

	var label *VersionLabel
	if len(match[4]) != 0 {
		label = versionLabel(match[4])
	}

	return NewVersion(num[0], num[1], num[2], label), nil
}

func versionLabel(str string) *VersionLabel {
	var label VersionLabel
	copy(label[:], str[:])
	return &label
}
