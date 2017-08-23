package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationsGlobalidOrgmembersIncludesuborgsPostReqBody struct {
	Globalid string `json:"globalid" validate:"nonzero"`
}

func (s OrganizationsGlobalidOrgmembersIncludesuborgsPostReqBody) Validate() error {

	return validator.Validate(s)
}
