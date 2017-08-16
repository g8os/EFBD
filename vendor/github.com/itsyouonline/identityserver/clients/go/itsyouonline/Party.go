package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type Party struct {
	Name string `json:"name" validate:"nonzero"`
	Type string `json:"type" validate:"nonzero"`
}

func (s Party) Validate() error {

	return validator.Validate(s)
}
