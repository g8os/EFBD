package rest

import (
	"gopkg.in/validator.v2"
)

type StoreStat struct {
	Size int64 `json:"size" validate:"min=0,nonzero"`
}

func (s StoreStat) Validate() error {

	return validator.Validate(s)
}
