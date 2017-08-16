package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationsGlobalidIsmemberUsernameGetRespBody struct {
	IsMember bool `json:"IsMember" validate:"nonzero"`
}

func (s OrganizationsGlobalidIsmemberUsernameGetRespBody) Validate() error {

	return validator.Validate(s)
}
