package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type VM struct {
	Cpu    int          `json:"cpu" validate:"nonzero"`
	Disks  []VDiskLink  `json:"disks" validate:"nonzero"`
	Id     string       `json:"id" validate:"nonzero"`
	Memory int          `json:"memory" validate:"nonzero"`
	Nics   []NicLink    `json:"nics" validate:"nonzero"`
	Status EnumVMStatus `json:"status" validate:"nonzero"`
}

func (s VM) Validate() error {

	return validator.Validate(s)
}
