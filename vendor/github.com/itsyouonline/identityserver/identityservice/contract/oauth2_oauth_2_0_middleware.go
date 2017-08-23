package contract

import (
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	contractdb "github.com/itsyouonline/identityserver/db/contract"
	"github.com/itsyouonline/identityserver/identityservice/security"
	"github.com/itsyouonline/identityserver/oauthservice"
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

		var atscopestring string
		var username string
		accessToken := om.GetAccessToken(r)
		if accessToken != "" {
			//TODO: cache
			oauthMgr := oauthservice.NewManager(r)
			at, err := oauthMgr.GetAccessToken(accessToken)
			if err != nil {
				log.Error(err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
			if at == nil {
				http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
				return
			}
			atscopestring = at.Scope
			username = at.Username
		} else {
			w.WriteHeader(401)
			return
		}
		scopes := []string{}

		contractID := mux.Vars(r)["contractId"]
		contractMngr := contractdb.NewManager(r)
		isParticipant, err := contractMngr.IsParticipant(contractID, username)
		if err != nil {
			log.Error(err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		if isParticipant {
			scopes = append(scopes, "contract:participant")
			scopes = append(scopes, "contract:read")
		}
		log.Debug("Available scopes: ", scopes)
		log.Debug("Atscopestring scope: ", atscopestring)

		// check scopes
		if !om.CheckScopes(scopes) {
			w.WriteHeader(403)
			return
		}

		next.ServeHTTP(w, r)
	})
}
