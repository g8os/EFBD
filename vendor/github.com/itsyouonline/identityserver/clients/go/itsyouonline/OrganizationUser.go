package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationUser struct {
	Missingscopes []string `json:"missingscopes" validate:"nonzero"`
	Role          string   `json:"role" validate:"nonzero"`
	Username      string   `json:"username" validate:"nonzero"`
}

func (s OrganizationUser) Validate() error {

	return validator.Validate(s)
}
