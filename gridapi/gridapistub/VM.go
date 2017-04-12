package gridapistub

import (
	"gopkg.in/validator.v2"
)

type VM struct {
	Cpu    int          `json:"cpu" validate:"nonzero"`
	Disk   []VDiskLink  `json:"disk" validate:"nonzero"`
	Id     string       `json:"id" validate:"nonzero"`
	Memory int          `json:"memory" validate:"nonzero"`
	Name   string       `json:"name" validate:"nonzero"`
	Nic    []NicLink    `json:"nic" validate:"nonzero"`
	Status EnumVMStatus `json:"status" validate:"nonzero"`
}

func (s VM) Validate() error {

	return validator.Validate(s)
}
