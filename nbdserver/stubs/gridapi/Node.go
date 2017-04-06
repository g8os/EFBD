package gridapi

import (
	"gopkg.in/validator.v2"
)

// Node node in the g8os grid
type Node struct {
	Hostname string         `json:"hostname" validate:"nonzero"`
	Id       string         `json:"id" validate:"nonzero"`
	Status   EnumNodeStatus `json:"status" validate:"nonzero"`
}

func (s Node) Validate() error {

	return validator.Validate(s)
}
