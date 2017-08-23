package organization

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrganizationValidation(t *testing.T) {
	type testcase struct {
		org   *Organization
		valid bool
	}
	testcases := []testcase{
		{org: &Organization{Globalid: ""}, valid: false},
		{org: &Organization{Globalid: "ab"}, valid: false},
		{org: &Organization{Globalid: "a♥"}, valid: false},
		{org: &Organization{Globalid: "abc"}, valid: true},
		{org: &Organization{Globalid: "abc:"}, valid: false},
		{org: &Organization{Globalid: "abc,"}, valid: false},
		{org: &Organization{Globalid: strings.Repeat("1", 150)}, valid: true},
		{org: &Organization{Globalid: strings.Repeat("1", 151)}, valid: false},
	}
	for _, test := range testcases {
		assert.Equal(t, test.valid, test.org.IsValid(), test.org.Globalid)
	}
}
