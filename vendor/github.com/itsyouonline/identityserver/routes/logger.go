package routes

import (
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
)

func LoggingMiddleware(h http.Handler) http.Handler {
	return handlers.LoggingHandler(log.StandardLogger().Out, h)
}
