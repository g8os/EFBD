package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernameNamePutReqBody struct {
	Firstname string `json:"firstname" validate:"nonzero"`
	Lastname  string `json:"lastname" validate:"nonzero"`
}

func (s UsersUsernameNamePutReqBody) Validate() error {

	return validator.Validate(s)
}
