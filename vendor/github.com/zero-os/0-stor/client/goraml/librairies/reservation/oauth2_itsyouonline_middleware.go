package reservation

import (
	"crypto/ecdsa"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/dgrijalva/jwt-go"
)

// Oauth2itsyouonlineMiddleware is oauth2 middleware for itsyouonline
type Oauth2itsyouonlineMiddleware struct {
	describedBy string
	field       string
	scopes      []string
}

var JWTPublicKey *ecdsa.PublicKey

const (
	oauth2ServerPublicKey = `` // fill it with oauth2 server public key
)

func init() {
	var err error

	if len(oauth2ServerPublicKey) == 0 {
		return
	}
	JWTPublicKey, err = jwt.ParseECPublicKeyFromPEM([]byte(oauth2ServerPublicKey))
	if err != nil {
		log.Fatalf("failed to parse pub key:%v", err)
	}

}

// NewOauth2itsyouonlineMiddlewarecreate new Oauth2itsyouonlineMiddleware struct
func NewOauth2itsyouonlineMiddleware(scopes []string) *Oauth2itsyouonlineMiddleware {
	om := Oauth2itsyouonlineMiddleware{
		scopes: scopes,
	}

	om.describedBy = "headers"
	om.field = "Authorization"

	return &om
}

// CheckScopes checks whether user has needed scopes
func (om *Oauth2itsyouonlineMiddleware) CheckScopes(scopes []string) bool {
	if len(om.scopes) == 0 {
		return true
	}

	for _, allowed := range om.scopes {
		for _, scope := range scopes {
			if scope == allowed {
				return true
			}
		}
	}
	return false
}

// Handler return HTTP handler representation of this middleware
func (om *Oauth2itsyouonlineMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var accessToken string
		var err error

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

		var scopes []string
		if len(oauth2ServerPublicKey) > 0 {
			scopes, err = om.checkJWTGetScope(accessToken)
			if err != nil {
				w.WriteHeader(403)
				return
			}
		}

		// check scopes
		if !om.CheckScopes(scopes) {
			w.WriteHeader(403)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// check JWT token and get it's scopes
func (om *Oauth2itsyouonlineMiddleware) checkJWTGetScope(tokenStr string) ([]string, error) {
	jwtStr := strings.TrimSpace(strings.TrimPrefix(tokenStr, "Bearer"))
	token, err := jwt.Parse(jwtStr, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodES384 {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return JWTPublicKey, nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !(ok && token.Valid) {
		return nil, fmt.Errorf("invalid token")
	}

	var scopes []string
	for _, v := range claims["scope"].([]interface{}) {
		scopes = append(scopes, v.(string))
	}
	return scopes, nil
}
