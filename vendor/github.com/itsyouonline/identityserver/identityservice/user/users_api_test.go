package user

import (
	"strings"
	"testing"

	"github.com/itsyouonline/identityserver/db/user"
	"github.com/stretchr/testify/assert"
)

func TestLabelValidation(t *testing.T) {
	type testcase struct {
		label string
		valid bool
	}
	testcases := []testcase{
		{label: "", valid: false},
		{label: "a", valid: false},
		{label: "ab", valid: true},
		{label: "abc", valid: true},
		{label: "abc- _", valid: true},
		{label: "abc%", valid: false},
		{label: strings.Repeat("1", 50), valid: true},
		{label: strings.Repeat("1", 51), valid: false},
	}
	for _, test := range testcases {
		assert.Equal(t, test.valid, user.IsValidLabel(test.label), test.label)
	}
}

func TestUsernameValidation(t *testing.T) {
	type testcase struct {
		username string
		valid    bool
	}
	testcases := []testcase{
		{username: "", valid: false},
		{username: "a", valid: false},
		{username: "ab", valid: true},
		{username: "abc", valid: true},
		{username: "ABC", valid: false},
		{username: "abc- _", valid: true},
		{username: "abb%", valid: false},
		{username: strings.Repeat("1", 30), valid: true},
		{username: strings.Repeat("1", 31), valid: false},
	}
	for _, test := range testcases {
		assert.Equal(t, test.valid, user.ValidateUsername(test.username), test.username)
	}
}
