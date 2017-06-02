package zerodisk

import "fmt"

var (
	// CurrentVersion represents the current global
	// version of the zerodisk modules
	CurrentVersion = NewVersion(1, 1, 0, VersionStageAlpha)
)

// NewVersion creates a new version
func NewVersion(major, minor, patch uint8, stage VersionStage) Version {
	return (Version(major) << 24) |
		(Version(minor) << 16) |
		(Version(patch) << 8) |
		Version(stage)
}

// Version defines the semantic version,
// used by Client and Server.
type Version uint32

// Compare returns an integer comparing this version
// with another version. { lt=-1 ; eq=0 ; gt=1 }
func (v Version) Compare(other Version) int {
	verA := v & versionBitMask
	verB := other & versionBitMask

	// are the actual versions not equal?
	if verA < verB {
		return -1
	} else if verA > verB {
		return 1
	}

	// actual versions are equal, so let's see if one
	// of them is in the non-production stage,
	// in which case the 1 in production is greater
	// then the one in prerelease or lower

	preA := VersionStage(v)
	preB := VersionStage(other)

	if preA != VersionStageLive && preB == VersionStageLive {
		return -1
	} else if preA == VersionStageLive && preB != VersionStageLive {
		return 1
	}

	// concidered to be equal versions
	return 0
}

// UInt32 returns the integral version
// of this Tlog Version
func (v Version) UInt32() uint32 {
	return uint32(v)
}

// String returns the string version
// of this Tlog Version
func (v Version) String() string {
	str := fmt.Sprintf("%d.%d.%d",
		v>>24,        // major
		(v>>16)&0xFF, // minor
		(v>>8)&0xFF,  // patch
	)

	// optional version stage
	if stage := VersionStage(v); stage != VersionStageLive {
		str += "-"
		switch stage {
		case VersionStageRC:
			str += strVersionStageRC
		case VersionStageBeta:
			str += strVersionStageBeta
		case VersionStageAlpha:
			str += strVersionStageAlpha
		default:
			str += strVersionStageDev
		}
	}

	return str
}

// VersionStage defines the stage of the version
type VersionStage uint8

// The different version stages
const (
	VersionStageLive VersionStage = 1 << iota
	VersionStageRC
	VersionStageBeta
	VersionStageAlpha
	VersionStageDev
)

// The different version stages in (short) string format
const (
	strVersionStageRC    = "rc"
	strVersionStageBeta  = "beta"
	strVersionStageAlpha = "alpha"
	strVersionStageDev   = "dev"
)

const (
	// used to remove the version stage from a version
	versionBitMask = Version(0xFFFFFF00)
)
