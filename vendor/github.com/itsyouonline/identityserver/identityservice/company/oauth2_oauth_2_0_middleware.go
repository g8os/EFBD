package company

import (
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/itsyouonline/identityserver/identityservice/security"
)

// Oauth2oauth_2_0Middleware is oauth2 middleware for oauth_2_0
type Oauth2oauth_2_0Middleware struct {
	security.OAuth2Middleware
}

// newOauth2oauth_2_0Middlewarecreate new Oauth2oauth_2_0Middleware struct
func newOauth2oauth_2_0Middleware(scopes []string) *Oauth2oauth_2_0Middleware {
	om := Oauth2oauth_2_0Middleware{}
	om.Scopes = scopes

	return &om
}

// CheckScopes checks whether user has needed scopes
func (om *Oauth2oauth_2_0Middleware) CheckScopes(scopes []string) bool {
	if len(om.Scopes) == 0 {
		return true
	}

	for _, allowed := range om.Scopes {
		for _, scope := range scopes {
			if scope == allowed {
				return true
			}
		}
	}
	return false
}

// Handler return HTTP handler representation of this middleware
func (om *Oauth2oauth_2_0Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accessToken := om.GetAccessToken(r)
		if accessToken == "" {
			w.WriteHeader(401)
			return
		}

		// TODO: WRITE codes to check user's scopes on this company
		scopes := []string{}
		log.Debug("Available scopes: ", scopes)

		// check scopes
		if !om.CheckScopes(scopes) {
			w.WriteHeader(403)
			return
		}

		next.ServeHTTP(w, r)
	})
}
