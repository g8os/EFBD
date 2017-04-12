package gridapistub

import (
	"gopkg.in/validator.v2"
)

// Arguments to join a Zerotier network
type ZerotierJoin struct {
	Nwid string `json:"nwid" validate:"nonzero"`
}

func (s ZerotierJoin) Validate() error {

	return validator.Validate(s)
}
