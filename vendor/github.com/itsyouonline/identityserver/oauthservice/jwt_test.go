package oauthservice

import (
	"testing"

	"github.com/itsyouonline/identityserver/credentials/oauth2"
	"github.com/stretchr/testify/assert"
)

func TestJWTScopesAreAllowed(t *testing.T) {
	type testcase struct {
		allowed   string
		requested string
		valid     bool
	}
	testcases := []testcase{
		testcase{allowed: "", requested: "", valid: true},
		testcase{allowed: "", requested: "user:memberof:org1", valid: false},
		testcase{allowed: "user:memberof:org1", requested: "", valid: true},
		testcase{allowed: "user:memberof:org2", requested: "user:memberof:org1", valid: false},
		testcase{allowed: "user:memberof:org1, user:memberof:org2", requested: "user:memberof:org1", valid: true},
		testcase{allowed: "user:memberof:org1", requested: "user:memberof:org1, user:memberof:org2", valid: false},
		testcase{allowed: "user:admin", requested: "user:memberof:org1", valid: true},
	}
	for _, test := range testcases {
		valid := jwtScopesAreAllowed(oauth2.SplitScopeString(test.allowed), oauth2.SplitScopeString(test.requested))
		assert.Equal(t, test.valid, valid, "Allowed: \"%s\" - Requested: \"%s\"", test.allowed, test.requested)
	}
}

func TestStripOfflineAccess(t *testing.T) {
	testcase := []string{"test", "offline_access"}
	resultingScopes, offlineAccessRequested := stripOfflineAccess(testcase)
	assert.NotContains(t, resultingScopes, "offline_access", "the offline_access scope should be stripped")
	assert.Contains(t, resultingScopes, "test", "the test scope should not be stripped")
	assert.True(t, offlineAccessRequested, "offline_access was requested")
}
