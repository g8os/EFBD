package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernameOrganizationsGetRespBody struct {
	Member []string `json:"member" validate:"nonzero"`
	Owner  []string `json:"owner" validate:"nonzero"`
}

func (s UsersUsernameOrganizationsGetRespBody) Validate() error {

	return validator.Validate(s)
}
