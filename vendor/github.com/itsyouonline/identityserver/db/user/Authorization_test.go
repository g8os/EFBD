package user

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScopesAreAuthorized(t *testing.T) {
	type testcase struct {
		a          Authorization
		s          string
		authorized bool
	}
	testcases := []testcase{
		testcase{a: Authorization{}, s: "user:memberof:orgid1", authorized: false},
		testcase{a: Authorization{Organizations: []string{"orgid"}}, s: "user:memberof:orgid", authorized: true},
		testcase{a: Authorization{Organizations: []string{"orgid.suborg"}}, s: "user:memberof:orgid.suborg", authorized: true},
		testcase{a: Authorization{Organizations: []string{"orgid1", "orgid2"}}, s: "user:memberof:orgid1, user:memberof:orgid2", authorized: true},
		testcase{a: Authorization{Organizations: []string{"orgid1", "orgid3"}}, s: "user:memberof:orgid1, user:memberof:orgid2", authorized: false},
		testcase{a: Authorization{}, s: "user:github", authorized: false},
		testcase{a: Authorization{Github: true}, s: "user:github", authorized: true},
		testcase{a: Authorization{}, s: "user:facebook", authorized: false},
		testcase{a: Authorization{Facebook: true}, s: "user:facebook", authorized: true},

		testcase{a: Authorization{Addresses: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "billing"}}}, s: "user:address:billing", authorized: true},
		testcase{a: Authorization{Addresses: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "billing"}}}, s: "user:address:home", authorized: false},
		testcase{a: Authorization{Addresses: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "billing"}}}, s: "user:address", authorized: true},
		testcase{a: Authorization{Addresses: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: ""}}}, s: "user:address", authorized: true},

		testcase{a: Authorization{BankAccounts: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "billing"}}}, s: "user:bankaccount:billing", authorized: true},
		testcase{a: Authorization{BankAccounts: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "billing"}}}, s: "user:bankaccount:home", authorized: false},
		testcase{a: Authorization{BankAccounts: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "billing"}}}, s: "user:bankaccount", authorized: true},
		testcase{a: Authorization{BankAccounts: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: ""}}}, s: "user:bankaccount", authorized: true},

		testcase{a: Authorization{EmailAddresses: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "main"}}}, s: "user:email:main", authorized: true},
		testcase{a: Authorization{EmailAddresses: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "main"}}}, s: "user:email:home", authorized: false},
		testcase{a: Authorization{EmailAddresses: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "main"}}}, s: "user:email", authorized: true},
		testcase{a: Authorization{EmailAddresses: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: ""}}}, s: "user:email", authorized: true},

		testcase{a: Authorization{Phonenumbers: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "main"}}}, s: "user:phone:main", authorized: true},
		testcase{a: Authorization{Phonenumbers: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "main"}}}, s: "user:phone:home", authorized: false},
		testcase{a: Authorization{Phonenumbers: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: "main"}}}, s: "user:phone", authorized: true},
		testcase{a: Authorization{Phonenumbers: []AuthorizationMap{AuthorizationMap{RealLabel: "home", RequestedLabel: ""}}}, s: "user:phone", authorized: true},
	}
	for _, test := range testcases {
		requestedScopes := strings.Split(test.s, ",")
		authorizedScopes := test.a.FilterAuthorizedScopes(requestedScopes)
		assert.Equal(t, test.authorized, len(requestedScopes) == len(authorizedScopes), test.s)
	}
}
