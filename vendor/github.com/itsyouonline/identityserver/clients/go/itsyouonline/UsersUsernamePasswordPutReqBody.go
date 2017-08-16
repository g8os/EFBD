package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernamePasswordPutReqBody struct {
	Currentpassword string `json:"currentpassword" validate:"nonzero"`
	Newpassword     string `json:"newpassword" validate:"nonzero"`
}

func (s UsersUsernamePasswordPutReqBody) Validate() error {

	return validator.Validate(s)
}
