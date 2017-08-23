package routes

import (
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"github.com/itsyouonline/identityserver/db"
	"github.com/itsyouonline/identityserver/identityservice"
	"github.com/itsyouonline/identityserver/oauthservice"
	"github.com/itsyouonline/identityserver/siteservice"
)

//GetRouter contructs the router hierarchy and registers all handlers and middleware
func GetRouter(sc *siteservice.Service, is *identityservice.Service, oauthsc *oauthservice.Service) http.Handler {
	r := mux.NewRouter().StrictSlash(true)

	sc.AddRoutes(r)
	sc.InitModels()

	apiRouter := r.PathPrefix("/api").Subrouter()
	is.AddRoutes(apiRouter)
	oauthsc.AddRoutes(r)

	// Add middlewares
	router := NewRouter(r)

	dbmw := db.DBMiddleware()
	recovery := handlers.RecoveryHandler()

	router.Use(recovery, LoggingMiddleware, dbmw, sc.SetWebUserMiddleWare)

	return router.Handler()
}
