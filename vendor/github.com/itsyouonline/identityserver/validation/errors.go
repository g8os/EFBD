package validation

import (
	"errors"
)

var (
	//ErrInvalidCode denotes that the supplied code is invalid
	ErrInvalidCode = errors.New("Invalid code")
	//ErrInvalidOrExpiredKey denotes that the key is not found, it can be invalid or expired
	ErrInvalidOrExpiredKey = errors.New("Invalid key")
)
