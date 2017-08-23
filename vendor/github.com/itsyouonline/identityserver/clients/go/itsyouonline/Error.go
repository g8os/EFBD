package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type Error struct {
	Error string `json:"error" validate:"nonzero"`
}

func (s Error) Validate() error {

	return validator.Validate(s)
}
