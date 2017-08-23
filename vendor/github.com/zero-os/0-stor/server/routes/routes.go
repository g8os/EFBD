package routes

import (
	"bytes"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/api/rest"
)

func LoggingMiddleware(h http.Handler) http.Handler {
	return handlers.LoggingHandler(log.StandardLogger().Out, h)
}

func RecoveryHandler(h http.Handler) http.Handler {
	return handlers.RecoveryHandler()(h)
}

func GetRouter(db db.DB) http.Handler {
	r := mux.NewRouter()

	// home page
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data, err := Asset("sdstor.html")
		if err != nil {
			w.WriteHeader(404)
			return
		}
		datareader := bytes.NewReader(data)
		http.ServeContent(w, r, "index.html", time.Now(), datareader)
	})

	api := rest.NewNamespaceAPI(db)
	rest.NamespacesInterfaceRoutes(r, api, db)

	router := NewRouter(r)
	router.Use(LoggingMiddleware)
	router.Use(RecoveryHandler)

	return router.Handler()
}
