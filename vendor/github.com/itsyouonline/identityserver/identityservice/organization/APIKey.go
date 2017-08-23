package organization

import (
	"regexp"

	"github.com/itsyouonline/identityserver/oauthservice"
	"gopkg.in/validator.v2"
)

type APIKey struct {
	CallbackURL                string `json:"callbackURL,omitempty" validate:"max=250"`
	ClientCredentialsGrantType bool   `json:"clientCredentialsGrantType,omitempty"`
	Label                      string `json:"label" validate:"min=2,max=50, pattern=^[a-zA-Z\d\-_\s]{2,50}$"`
	Secret                     string `json:"secret,omitempty" validate:"max=250,nonzero"`
}

//FromOAuthClient creates an APIKey instance from an oauthservice.Oauth2Client
func FromOAuthClient(client *oauthservice.Oauth2Client) APIKey {
	apiKey := APIKey{
		CallbackURL:                client.CallbackURL,
		ClientCredentialsGrantType: client.ClientCredentialsGrantType,
		Label:  client.Label,
		Secret: client.Secret,
	}
	return apiKey
}

func (a APIKey) Validate() bool {
	return validator.Validate(a) == nil && regexp.MustCompile(`^[a-zA-Z\d\-_\s]{2,50}$`).MatchString(a.Label)
}
