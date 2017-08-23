package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type Membership struct {
	Role     string `json:"role" validate:"nonzero"`
	Username string `json:"username" validate:"nonzero"`
}

func (s Membership) Validate() error {

	return validator.Validate(s)
}
