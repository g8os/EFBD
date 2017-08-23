package siteservice

import (
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/sessions"

	"time"

	"github.com/gorilla/context"
)

//SessionType is used to define the type of session
type SessionType int

const (
	//SessionForRegistration is the short anynymous session used during registration
	SessionForRegistration SessionType = iota
	//SessionInteractive is the session of an authenticated user on the itsyou.online website
	SessionInteractive SessionType = iota
	//SessionLogin is the session during the login flow
	SessionLogin SessionType = iota
	//SessionOauth is the session during an oauth flow
	SessionOauth SessionType = iota
)

//initializeSessionStore creates a cookieStore
// mageAge is the maximum age in seconds
func initializeSessionStore(cookieSecret string, maxAge int) (sessionStore *sessions.CookieStore) {
	sessionStore = sessions.NewCookieStore([]byte(cookieSecret))
	sessionStore.Options.HttpOnly = true

	sessionStore.Options.Secure = true
	sessionStore.Options.MaxAge = maxAge
	return
}

func (service *Service) initializeSessions(cookieSecret string) {
	service.Sessions = make(map[SessionType]*sessions.CookieStore)

	service.Sessions[SessionForRegistration] = initializeSessionStore(cookieSecret, 10*60)
	service.Sessions[SessionInteractive] = initializeSessionStore(cookieSecret, 10*60)
	service.Sessions[SessionLogin] = initializeSessionStore(cookieSecret, 5*60)
	service.Sessions[SessionOauth] = initializeSessionStore(cookieSecret, 10*60)

}

//GetSession returns the a session of the specified kind and a specific name
func (service *Service) GetSession(request *http.Request, kind SessionType, name string) (*sessions.Session, error) {
	return service.Sessions[kind].Get(request, name)
}

//SetLoggedInUser creates a session for an authenticated user and clears the login session
func (service *Service) SetLoggedInUser(w http.ResponseWriter, request *http.Request, username string) (err error) {
	authenticatedSession, err := service.GetSession(request, SessionInteractive, "authenticatedsession")
	if err != nil {
		log.Error(err)
		return
	}
	authenticatedSession.Values["username"] = username

	//TODO: rework this, is not really secure I think
	// Set user cookie after successful login
	cookie := &http.Cookie{
		Name:  "itsyou.online.user",
		Path:  "/",
		Value: username,
	}
	http.SetCookie(w, cookie)

	// Clear login session
	loginCookie := &http.Cookie{
		Name:    "loginsession",
		Path:    "/",
		Value:   "",
		Expires: time.Unix(1, 0),
	}
	http.SetCookie(w, loginCookie)

	return
}

// SetOauthUser creates a protected session after an oauth flow and clears the login session
// Also sets the clientID and state
func (service *Service) SetLoggedInOauthUser(w http.ResponseWriter, r *http.Request, username /*, clientId, state*/ string) (err error) {
	oauthSession, err := service.GetSession(r, SessionOauth, "oauthsession")
	if err != nil {
		log.Error(err)
		return
	}
	oauthSession.Values["username"] = username

	// No need to set a user cookie since we don't pass through the UI

	// Clear login session
	loginCookie := &http.Cookie{
		Name:    "loginsession",
		Path:    "/",
		Value:   "",
		Expires: time.Unix(1, 0),
	}
	http.SetCookie(w, loginCookie)

	return
}

//SetAPIAccessToken sets the api access token in a cookie
//TODO: is not safe to do. Now there are also two ways of passing tokens to the client
func (service *Service) SetAPIAccessToken(w http.ResponseWriter, token string) (err error) {
	// Set token cookie
	cookie := &http.Cookie{
		Name:  "itsyou.online.apitoken",
		Path:  "/",
		Value: token,
	}
	http.SetCookie(w, cookie)

	return
}

//GetLoggedInUser returns an authenticated user, or an empty string if there is none
func (service *Service) GetLoggedInUser(request *http.Request, w http.ResponseWriter) (username string, err error) {
	authenticatedSession, err := service.GetSession(request, SessionInteractive, "authenticatedsession")
	if err != nil {
		log.Error(err)
		return
	}
	err = authenticatedSession.Save(request, w)
	if err != nil {
		log.Error(err)
		return
	}
	savedusername := authenticatedSession.Values["username"]
	if savedusername != nil {
		username, _ = savedusername.(string)
	}
	return
}

// GetOauthUser returns the user in an oauth session, or an empty string if there is none
func (service *Service) GetOauthUser(r *http.Request, w http.ResponseWriter) (username string, err error) {
	oauthSession, err := service.GetSession(r, SessionOauth, "oauthsession")
	if err != nil {
		log.Error(err)
		return
	}
	err = oauthSession.Save(r, w)
	if err != nil {
		log.Error(err)
		return
	}
	savedusername := oauthSession.Values["username"]
	if savedusername != nil {
		username, _ = savedusername.(string)
	}
	return
}

//SetWebUserMiddleWare puthe the authenticated user on the context
func (service *Service) SetWebUserMiddleWare(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if username, err := service.GetLoggedInUser(request, w); err == nil {
			context.Set(request, "webuser", username)
		}

		next.ServeHTTP(w, request)
	})
}
