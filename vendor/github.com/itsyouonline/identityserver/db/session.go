package db

import (
	"net/http"

	"github.com/gorilla/context"
	"gopkg.in/mgo.v2"
)

func GetDBSession(r *http.Request) *mgo.Session {
	session := context.Get(r, DB_SESSION)
	if session == nil {
		return nil
	}

	return session.(*mgo.Session)
}

func SetDBSession(r *http.Request) *mgo.Session {
	newSession := NewSession()

	if newSession == nil {
		return nil
	}

	context.Set(r, DB_SESSION, newSession)

	return newSession
}

func NewSession() *mgo.Session {
	if dbSession == nil {
		return nil
	}

	return dbSession.Copy()
}
