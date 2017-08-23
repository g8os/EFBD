package organization

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAPIKeyValidation(t *testing.T) {
	type testcase struct {
		apiKey APIKey
		valid  bool
	}
	cbUrl := "https://test.example.com/callback"
	s := "asdfafdsfhowpierqwpoiosdafjalksdfls"
	l := "labeltest"
	testCases := []testcase{
		{apiKey: APIKey{Label: l, CallbackURL: cbUrl, ClientCredentialsGrantType: true, Secret: s}, valid: true},
		{apiKey: APIKey{Label: "a", CallbackURL: cbUrl, ClientCredentialsGrantType: true, Secret: s}, valid: false},
		{apiKey: APIKey{Label: "ab", CallbackURL: cbUrl, ClientCredentialsGrantType: true, Secret: s}, valid: true},
		{apiKey: APIKey{Label: strings.Repeat("1", 50), CallbackURL: cbUrl, ClientCredentialsGrantType: true, Secret: s}, valid: true},
		{apiKey: APIKey{Label: strings.Repeat("1", 51), CallbackURL: cbUrl, ClientCredentialsGrantType: true, Secret: s}, valid: false},
		{apiKey: APIKey{Label: l, CallbackURL: "abcd", ClientCredentialsGrantType: true, Secret: s}, valid: true},
		{apiKey: APIKey{Label: l, CallbackURL: "https://test.com", ClientCredentialsGrantType: true, Secret: s}, valid: true},
		{apiKey: APIKey{Label: l, CallbackURL: strings.Repeat("1", 250), ClientCredentialsGrantType: true, Secret: s}, valid: true},
		{apiKey: APIKey{Label: l, CallbackURL: strings.Repeat("1", 251), ClientCredentialsGrantType: true, Secret: s}, valid: false},
		{apiKey: APIKey{Label: l, CallbackURL: cbUrl, ClientCredentialsGrantType: false, Secret: s}, valid: true},
		{apiKey: APIKey{Label: l, CallbackURL: cbUrl, ClientCredentialsGrantType: true, Secret: ""}, valid: false},
	}
	for _, test := range testCases {
		assert.Equal(t, test.valid, test.apiKey.Validate())
	}
}
