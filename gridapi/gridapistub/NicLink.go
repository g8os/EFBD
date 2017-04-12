package gridapistub

import (
	"gopkg.in/validator.v2"
)

// Definition of a virtual nic
type NicLink struct {
	Id         string          `json:"id" validate:"nonzero"`
	Macaddress string          `json:"macaddress" validate:"nonzero"`
	Type       EnumNicLinkType `json:"type" validate:"nonzero"`
}

func (s NicLink) Validate() error {

	return validator.Validate(s)
}
