package company

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompanyValidation(t *testing.T) {
	type testcase struct {
		company *Company
		valid   bool
	}
	testcases := []testcase{
		testcase{company: &Company{Globalid: ""}, valid: false},
		testcase{company: &Company{Globalid: "ab"}, valid: false},
		//	testcase{company: &Company{Globalid: "a♥"}, valid: false}, Let's just limit the amount of bytes for now
		testcase{company: &Company{Globalid: "abc"}, valid: true},
		testcase{company: &Company{Globalid: strings.Repeat("1", 150)}, valid: true},
		testcase{company: &Company{Globalid: strings.Repeat("1", 151)}, valid: false},
	}
	for _, test := range testcases {
		assert.Equal(t, test.valid, test.company.IsValid(), test.company.Globalid)
	}
}
