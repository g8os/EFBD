package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type ProcessSignal struct {
	Signal int `json:"signal" validate:"min=1,nonzero"`
}

func (s ProcessSignal) Validate() error {

	return validator.Validate(s)
}
