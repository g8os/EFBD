package itsyouonline

import (
	"gopkg.in/validator.v2"
)

// An avatar of a user
type Avatar struct {
	Label  Label  `json:"label" validate:"nonzero"`
	Source string `json:"source" validate:"nonzero"`
}

func (s Avatar) Validate() error {

	return validator.Validate(s)
}
