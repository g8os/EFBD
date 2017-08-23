package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationsGlobalidApikeysLabelPutReqBody struct {
	Type OrganizationAPIKey `json:"type" validate:"nonzero"`
}

func (s OrganizationsGlobalidApikeysLabelPutReqBody) Validate() error {

	return validator.Validate(s)
}
