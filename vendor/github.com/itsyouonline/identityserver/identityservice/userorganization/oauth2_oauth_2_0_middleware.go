package userorganization

import "github.com/itsyouonline/identityserver/identityservice/user"

// Oauth2oauth_2_0Middleware is just a wrapper for the user authorization middleware and follows the same rules for authorization
type Oauth2oauth_2_0Middleware struct {
	user.Oauth2oauth_2_0Middleware
}

// newOauth2oauth_2_0Middlewarecreate new Oauth2oauth_2_0Middleware struct
func newOauth2oauth_2_0Middleware(scopes []string) *Oauth2oauth_2_0Middleware {
	om := &Oauth2oauth_2_0Middleware{}
	om.Scopes = scopes

	return om
}
