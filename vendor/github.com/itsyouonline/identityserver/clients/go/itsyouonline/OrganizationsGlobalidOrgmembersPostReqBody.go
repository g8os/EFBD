package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationsGlobalidOrgmembersPostReqBody struct {
	Orgmember string `json:"orgmember" validate:"nonzero"`
}

func (s OrganizationsGlobalidOrgmembersPostReqBody) Validate() error {

	return validator.Validate(s)
}
