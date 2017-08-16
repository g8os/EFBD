package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type Member struct {
	Username string `json:"username" validate:"nonzero"`
}

func (s Member) Validate() error {

	return validator.Validate(s)
}
