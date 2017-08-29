package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationsGlobalidTransfersuborganizationPostReqBody struct {
	Globalid  string `json:"globalid" validate:"nonzero"`
	Newparent string `json:"newparent" validate:"nonzero"`
}

func (s OrganizationsGlobalidTransfersuborganizationPostReqBody) Validate() error {

	return validator.Validate(s)
}
