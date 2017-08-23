package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type ContractSigningRequest struct {
	ContractId string `json:"contractId" validate:"nonzero"`
	Party      string `json:"party" validate:"nonzero"`
}

func (s ContractSigningRequest) Validate() error {

	return validator.Validate(s)
}
