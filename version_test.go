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
		{NewVersion(0, 0, 0, nil), "0.0.0"},
		{NewVersion(0, 0, 1, nil), "0.0.1"},
		{NewVersion(0, 1, 0, nil), "0.1.0"},
		{NewVersion(1, 0, 0, nil), "1.0.0"},
		{NewVersion(1, 0, 0, versionLabel("beta-1")), "1.0.0-beta-1"},
		{NewVersion(1, 1, 0, nil), "1.1.0"},
		{NewVersion(1, 1, 0, versionLabel("abcdefgh")), "1.1.0-abcdefgh"},
		{NewVersion(1, 1, 0, nil), "1.1.0"},
		{NewVersion(1, 2, 3, nil), "1.2.3"},
		{NewVersion(1, 2, 3, versionLabel("alpha-8")), "1.2.3-alpha-8"},
		{NewVersion(4, 2, 0, nil), "4.2.0"},
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
		{NewVersion(0, 0, 0, nil), NewVersion(0, 0, 0, nil), 0},
		{NewVersion(0, 0, 1, nil), NewVersion(0, 0, 1, nil), 0},
		{NewVersion(0, 1, 0, nil), NewVersion(0, 1, 0, nil), 0},
		{NewVersion(0, 1, 0, versionLabel("foo")), NewVersion(0, 1, 0, nil), 0},
		{NewVersion(0, 1, 0, nil), NewVersion(0, 1, 0, versionLabel("foo")), 0},
		{NewVersion(0, 1, 0, versionLabel("foo")), NewVersion(0, 1, 0, versionLabel("foo")), 0},
		{NewVersion(1, 0, 0, nil), NewVersion(1, 0, 0, nil), 0},
		{NewVersion(1, 1, 0, nil), NewVersion(1, 1, 0, nil), 0},
		{NewVersion(3, 2, 1, nil), NewVersion(3, 2, 1, nil), 0},
		// different
		{NewVersion(2, 0, 0, nil), NewVersion(1, 12, 19, nil), 1},
		{NewVersion(1, 0, 0, nil), NewVersion(0, 1, 1, nil), 1},
		{NewVersion(1, 0, 1, nil), NewVersion(1, 0, 0, nil), 1},
		{NewVersion(1, 1, 1, nil), NewVersion(1, 1, 0, nil), 1},
		{NewVersion(0, 1, 0, nil), NewVersion(0, 0, 1, nil), 1},
		{NewVersion(0, 1, 1, nil), NewVersion(0, 1, 0, nil), 1},
		{NewVersion(0, 0, 1, nil), NewVersion(0, 0, 0, nil), 1},
		{NewVersion(1, 12, 19, nil), NewVersion(2, 0, 0, nil), -1},
		{NewVersion(0, 1, 1, nil), NewVersion(1, 0, 0, nil), -1},
		{NewVersion(1, 0, 0, nil), NewVersion(1, 0, 1, nil), -1},
		{NewVersion(1, 1, 0, nil), NewVersion(1, 1, 1, nil), -1},
		{NewVersion(0, 0, 1, nil), NewVersion(0, 1, 0, nil), -1},
		{NewVersion(0, 1, 0, nil), NewVersion(0, 1, 1, nil), -1},
		{NewVersion(0, 0, 0, nil), NewVersion(0, 0, 1, nil), -1},
	}

	for _, testCase := range testCases {
		result := testCase.verA.Compare(testCase.verB)
		assert.Equalf(t, testCase.expected, result, "%s v %s", testCase.verA, testCase.verB)
	}
}
