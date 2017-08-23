package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationsGlobalidOrgmembersIncludesuborgsPostRespBody struct {
	Type Organization `json:"type" validate:"nonzero"`
}

func (s OrganizationsGlobalidOrgmembersIncludesuborgsPostRespBody) Validate() error {

	return validator.Validate(s)
}
