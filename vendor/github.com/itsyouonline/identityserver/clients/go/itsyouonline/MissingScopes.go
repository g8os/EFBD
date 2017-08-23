package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type MissingScopes struct {
	Organization string   `json:"organization" validate:"nonzero"`
	Scopes       []string `json:"scopes" validate:"nonzero"`
}

func (s MissingScopes) Validate() error {

	return validator.Validate(s)
}
