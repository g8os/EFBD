package db

import (
	"errors"
	"net/http"
)

type DBHandler struct {
	handler http.Handler
}

func (d *DBHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	session := SetDBSession(r)

	if session == nil {
		panic(errors.New("Failed to retrieve a DB session!"))
	}

	defer func() {
		session.Close()
	}()

	d.handler.ServeHTTP(w, r)
}

func DBMiddleware() func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return &DBHandler{
			handler: h,
		}
	}
}
