package client

import (
	"gopkg.in/validator.v2"
)

type Namespace struct {
	Label string        `json:"label" validate:"min=5,max=128,regexp=^\w+$,nonzero"`
	Stats NamespaceStat `json:"stats,omitempty"`
}

func (s Namespace) Validate() error {

	return validator.Validate(s)
}
