package gridapi

import (
	"gopkg.in/validator.v2"
)

// Information about network interface
type NicInfo struct {
	Addrs        []string `json:"addrs" validate:"nonzero"`
	Flags        []string `json:"flags" validate:"nonzero"`
	Hardwareaddr string   `json:"hardwareaddr" validate:"nonzero"`
	Mtu          int      `json:"mtu" validate:"nonzero"`
	Name         string   `json:"name" validate:"nonzero"`
}

func (s NicInfo) Validate() error {

	return validator.Validate(s)
}
