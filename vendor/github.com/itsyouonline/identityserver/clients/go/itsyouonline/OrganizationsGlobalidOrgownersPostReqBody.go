package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationsGlobalidOrgownersPostReqBody struct {
	Orgowner string `json:"orgowner" validate:"nonzero"`
}

func (s OrganizationsGlobalidOrgownersPostReqBody) Validate() error {

	return validator.Validate(s)
}
