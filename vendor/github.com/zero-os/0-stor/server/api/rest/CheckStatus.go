package rest

import (
	"gopkg.in/validator.v2"
)

type CheckStatus struct {
	Id     string                `json:"id" validate:"min=1,max=128,regexp=^\w+$,nonzero"`
	Status EnumCheckStatusStatus `json:"status" validate:"nonzero"`
}

func (s CheckStatus) Validate() error {

	return validator.Validate(s)
}
