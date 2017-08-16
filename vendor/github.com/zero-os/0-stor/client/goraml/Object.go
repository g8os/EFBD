package client

import (
	"gopkg.in/validator.v2"
)

type Object struct {
	Data          string        `json:"data" validate:"nonzero"`
	Id            string        `json:"id" validate:"min=1,max=128,regexp=^\w+$,nonzero"`
	ReferenceList []ReferenceID `json:"referenceList" validate:"nonzero"`
}

func (s Object) Validate() error {

	return validator.Validate(s)
}
