package oauth2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//TestGetValidJWT tests the GetValidJWT function
func TestSplitScopeString(t *testing.T) {
	type testcase struct {
		ScopeString string
		Scopes      []string
	}
	tests := []testcase{
		{ScopeString: "", Scopes: []string{}},
		{ScopeString: "abcd,efgh", Scopes: []string{"abcd", "efgh"}},
		{ScopeString: "user:admin,", Scopes: []string{"user:admin"}},
	}
	for _, test := range tests {
		assert.Equal(t, test.Scopes, SplitScopeString(test.ScopeString), "Failed to split scopestring'"+test.ScopeString+"' correctly'")
	}
}
