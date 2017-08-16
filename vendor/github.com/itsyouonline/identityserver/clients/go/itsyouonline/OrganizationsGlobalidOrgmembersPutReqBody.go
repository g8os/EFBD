package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationsGlobalidOrgmembersPutReqBody struct {
	Org  string `json:"org" validate:"nonzero"`
	Role string `json:"role" validate:"nonzero"`
}

func (s OrganizationsGlobalidOrgmembersPutReqBody) Validate() error {

	return validator.Validate(s)
}
