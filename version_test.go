package zerodisk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionString(t *testing.T) {
	testCases := []struct {
		version  Version
		expected string
	}{
		{NewVersion(0, 0, 0, 0), "0.0.0-" + strVersionStageDev},
		{NewVersion(0, 0, 1, 0), "0.0.1-" + strVersionStageDev},
		{NewVersion(0, 1, 0, 0), "0.1.0-" + strVersionStageDev},
		{NewVersion(1, 0, 0, 0), "1.0.0-" + strVersionStageDev},
		{NewVersion(1, 1, 0, VersionStageDev), "1.1.0-" + strVersionStageDev},
		{NewVersion(1, 1, 0, VersionStageAlpha), "1.1.0-" + strVersionStageAlpha},
		{NewVersion(1, 1, 0, VersionStageBeta), "1.1.0-" + strVersionStageBeta},
		{NewVersion(1, 1, 0, VersionStageRC), "1.1.0-" + strVersionStageRC},
		{NewVersion(1, 1, 0, VersionStageLive), "1.1.0"},
		{NewVersion(1, 2, 3, VersionStageLive), "1.2.3"},
		{NewVersion(4, 2, 0, VersionStageLive), "4.2.0"},
	}

	for _, testCase := range testCases {
		str := testCase.version.String()
		assert.Equal(t, testCase.expected, str)
	}
}

func TestVersionCompare(t *testing.T) {
	testCases := []struct {
		verA, verB Version
		expected   int
	}{
		// equal
		{NewVersion(0, 0, 0, 0), NewVersion(0, 0, 0, 0), 0},
		{NewVersion(0, 0, 1, 0), NewVersion(0, 0, 1, 0), 0},
		{NewVersion(0, 1, 0, 0), NewVersion(0, 1, 0, 0), 0},
		{NewVersion(1, 0, 0, 0), NewVersion(1, 0, 0, 0), 0},
		{NewVersion(1, 1, 0, VersionStageAlpha), NewVersion(1, 1, 0, VersionStageAlpha), 0},
		// equal, even though version non-live-stage is different
		{NewVersion(0, 0, 0, VersionStageAlpha), NewVersion(0, 0, 0, VersionStageDev), 0},
		{NewVersion(0, 0, 0, VersionStageAlpha), NewVersion(0, 0, 0, VersionStageBeta), 0},
		{NewVersion(0, 0, 0, VersionStageRC), NewVersion(0, 0, 0, VersionStageBeta), 0},
		// different because of version stage
		{NewVersion(0, 0, 0, VersionStageLive), NewVersion(0, 0, 0, 0), 1},
		{NewVersion(0, 0, 0, 0), NewVersion(0, 0, 0, VersionStageLive), -1},
		// different because of actual version
		{NewVersion(2, 0, 0, 0), NewVersion(1, 12, 19, 0), 1},
		{NewVersion(1, 0, 0, 0), NewVersion(0, 1, 1, 0), 1},
		{NewVersion(1, 0, 1, 0), NewVersion(1, 0, 0, 0), 1},
		{NewVersion(1, 1, 1, 0), NewVersion(1, 1, 0, 0), 1},
		{NewVersion(0, 1, 0, 0), NewVersion(0, 0, 1, 0), 1},
		{NewVersion(0, 1, 1, 0), NewVersion(0, 1, 0, 0), 1},
		{NewVersion(0, 0, 1, 0), NewVersion(0, 0, 0, 0), 1},
		{NewVersion(1, 12, 19, 0), NewVersion(2, 0, 0, 0), -1},
		{NewVersion(0, 1, 1, 0), NewVersion(1, 0, 0, 0), -1},
		{NewVersion(1, 0, 0, 0), NewVersion(1, 0, 1, 0), -1},
		{NewVersion(1, 1, 0, 0), NewVersion(1, 1, 1, 0), -1},
		{NewVersion(0, 0, 1, 0), NewVersion(0, 1, 0, 0), -1},
		{NewVersion(0, 1, 0, 0), NewVersion(0, 1, 1, 0), -1},
		{NewVersion(0, 0, 0, 0), NewVersion(0, 0, 1, 0), -1},
	}

	for _, testCase := range testCases {
		result := testCase.verA.Compare(testCase.verB)
		assert.Equal(t, testCase.expected, result)
	}
}
