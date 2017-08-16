package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type RequiredScope struct {
	Accessscopes []string `json:"accessscopes" validate:"nonzero"`
	Scope        string   `json:"scope" validate:"max=1024,nonzero"`
}

func (s RequiredScope) Validate() error {

	return validator.Validate(s)
}
