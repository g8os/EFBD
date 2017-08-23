package db

import "errors"

const (
	DB_NAME    = "itsyouonline-idserver-db"
	DB_SESSION = "itsyouonline/identityserver/dbconnection"
)

var (
	//ErrDuplicate indicates the entry is invalid because of a primary key violation
	ErrDuplicate = errors.New("Duplicate")
)
