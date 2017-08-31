package rest

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/zero-os/0-stor/server/jwt"

	log "github.com/Sirupsen/logrus"

	"github.com/gorilla/mux"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/manager"
)

// Oauth2itsyouonlineMiddleware is oauth2 middleware for itsyouonline
type Oauth2itsyouonlineMiddleware struct {
	describedBy string
	field       string
	db          db.DB
}

// NewOauth2itsyouonlineMiddleware new Oauth2itsyouonlineMiddleware struct
func NewOauth2itsyouonlineMiddleware(db db.DB) *Oauth2itsyouonlineMiddleware {
	om := Oauth2itsyouonlineMiddleware{
		describedBy: "headers",
		field:       "Authorization",
		db:          db,
	}

	return &om
}

// GetExpectedScopes deduct the required scope based on the request method
func (om *Oauth2itsyouonlineMiddleware) GetExpectedScopes(r *http.Request, nsid string) ([]string, error) {
	permissions := map[string]string{
		"GET":    "read",
		"POST":   "write",
		"DELETE": "delete",
		"PUT":    "write",
		"HEAD":   "read",
	}

	perm, ok := permissions[r.Method]

	if !ok {
		return []string{}, fmt.Errorf("No permission specified for HTTP method %v", r.Method)
	}

	adminScope := strings.Replace(nsid, "_0stor_", ".0stor.", 1)

	return []string{
		// example :: first.0stor.gig.read
		fmt.Sprintf("%s.%s", adminScope, perm),
		//admin ::first.0stor.gig
		adminScope,
	}, nil
}

// Handler return HTTP handler representation of this middleware
func (om *Oauth2itsyouonlineMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		var accessToken string

		// access token checking
		if om.describedBy == "queryParameters" {
			accessToken = r.URL.Query().Get(om.field)
		} else if om.describedBy == "headers" {
			accessToken = r.Header.Get(om.field)
		}
		if accessToken == "" {
			w.WriteHeader(401)
			return
		}

		// nsid checking
		label, ok := mux.Vars(r)["nsid"]
		if !ok {
			w.WriteHeader(400)
			return
		}

		if err := jwt.ValidateNamespaceLabel(label); err != nil {
			log.Errorln(err.Error())
			w.WriteHeader(400)
			return
		}

		expectedScope, err := om.GetExpectedScopes(r, label)

		if err != nil {
			log.Errorln(err.Error())
			w.WriteHeader(400)
			return
		}

		// if len(oauth2ServerPublicKey) > 0 {
		userScopes, _, err := jwt.CheckJWTGetScopes(accessToken)
		if err != nil {
			log.Errorln(err.Error())
			log.Errorf("[IYO] User scopes : %v", userScopes)
			log.Errorf("[IYO] Expected scope : %v", expectedScope)
			w.WriteHeader(403)
			return
		}

		// check scopes
		if !jwt.CheckPermissions(expectedScope, userScopes) {
			w.WriteHeader(403)
			return
		}

		// Create namespace if not exists
		if err := manager.NewNamespaceManager(om.db).Create(label); err != nil {
			w.WriteHeader(500)
			return
		}
		// }
		next.ServeHTTP(w, r)
	})
}
