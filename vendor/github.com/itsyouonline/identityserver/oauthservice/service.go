package oauthservice

import (
	"crypto/ecdsa"
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

//SessionService declares a context where you can have a logged in user
type SessionService interface {
	//GetLoggedInUser returns an authenticated user, or an empty string if there is none
	GetLoggedInUser(request *http.Request, w http.ResponseWriter) (username string, err error)
	//GetOauthUser returns a user in a protected oauth session, or an empty string if there is none
	GetOauthUser(request *http.Request, w http.ResponseWriter) (username string, err error)
	//SetAPIAccessToken sets the api access token for this session
	SetAPIAccessToken(w http.ResponseWriter, token string) (err error)
}

//IdentityService provides some basic knowledge about authorizations required for the oauthservice
type IdentityService interface {
	//FilterAuthorizedScopes filters the requested scopes to the ones that are authorizated, if no authorization exists, authorizedScops is nil
	FilterAuthorizedScopes(r *http.Request, username string, grantedTo string, requestedscopes []string) (authorizedScopes []string, err error)
	//FilterPossibleScopes filters the requestedScopes to the relevant ones that are possible
	// For example, a `user:memberof:orgid1` is not possible if the user is not a member the `orgid1` organization and there is no outstanding invite for this organization
	// If allowInvitations is true, invitations to organizations allows the "user:memberof:organization" as possible scopes
	FilterPossibleScopes(r *http.Request, username string, requestedScopes []string, allowInvitations bool) (possibleScopes []string, err error)
}

//Service is the oauthserver http service
type Service struct {
	sessionService  SessionService
	identityService IdentityService
	router          *mux.Router
	jwtSigningKey   *ecdsa.PrivateKey
}

//NewService creates and initializes a Service
func NewService(sessionService SessionService, identityService IdentityService, ecdsaKey *ecdsa.PrivateKey) (service *Service, err error) {
	service = &Service{sessionService: sessionService, identityService: identityService, jwtSigningKey: ecdsaKey}
	return
}

const (
	//AuthorizationGrantCodeType is the requested response_type for an 'authorization code' oauth2 flow
	AuthorizationGrantCodeType = "code"
	//ClientCredentialsGrantCodeType is the requested grant_type for a 'client credentials' oauth2 flow
	ClientCredentialsGrantCodeType = "client_credentials"
)

//GetWebuser returns the authenticated user if any or an empty string if not
func (service *Service) GetWebuser(r *http.Request, w http.ResponseWriter) (username string, err error) {
	username, err = service.sessionService.GetLoggedInUser(r, w)
	return
}

//GetOauthUser returns a user in a protected oauth session, or an empty string if there is none
func (service *Service) GetOauthUser(r *http.Request, w http.ResponseWriter) (username string, err error) {
	username, err = service.sessionService.GetOauthUser(r, w)
	return
}

func (service *Service) filterPossibleScopes(r *http.Request, username string, requestedScopes []string, allowInvitations bool) (possibleScopes []string, err error) {
	log.Debug("Filtering requested scopes: ", requestedScopes)
	possibleScopes, err = service.identityService.FilterPossibleScopes(r, username, requestedScopes, allowInvitations)
	log.Debug("Possible scopes: ", possibleScopes)
	return
}

//AddRoutes adds the routes and handlerfunctions to the router
func (service *Service) AddRoutes(router *mux.Router) {
	service.router = router
	router.HandleFunc("/v1/oauth/authorize", service.AuthorizeHandler).Methods("GET")
	router.HandleFunc("/v1/oauth/authorize",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Allow", "GET")
		}).Methods("OPTIONS")

	router.HandleFunc("/v1/oauth/access_token", service.AccessTokenHandler).Methods("POST")
	router.HandleFunc("/v1/oauth/access_token",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Allow", "POST")
			// Allow cors
			w.Header().Add("Access-Control-Allow-Origin", "*")
			w.Header().Add("Access-Control-Allow-Methods", "POST")
			// Allow all requested headers, we do not use them anyway
			w.Header().Add("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
		}).Methods("OPTIONS")

	router.HandleFunc("/v1/oauth/jwt", service.JWTHandler).Methods("POST", "GET")
	router.HandleFunc("/v1/oauth/jwt",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Allow", "GET,POST")
		}).Methods("OPTIONS")
	router.HandleFunc("/v1/oauth/jwt/refresh", service.RefreshJWTHandler).Methods("POST", "GET")
	router.HandleFunc("/v1/oauth/jwt/refresh",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Allow", "GET,POST")
		}).Methods("OPTIONS")

	InitModels()
}
