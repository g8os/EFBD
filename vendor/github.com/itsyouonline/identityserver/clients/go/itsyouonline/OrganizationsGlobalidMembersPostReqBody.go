package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationsGlobalidMembersPostReqBody struct {
	Searchstring string `json:"searchstring" validate:"nonzero"`
}

func (s OrganizationsGlobalidMembersPostReqBody) Validate() error {

	return validator.Validate(s)
}
