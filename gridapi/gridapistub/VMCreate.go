package gridapistub

import (
	"encoding/json"
	"gopkg.in/validator.v2"
)

type VMCreate struct {
	Cpu             int             `json:"cpu" validate:"nonzero"`
	Disk            []VDiskLink     `json:"disk" validate:"nonzero"`
	Memory          int             `json:"memory" validate:"nonzero"`
	Name            string          `json:"name" validate:"nonzero"`
	Nic             []NicLink       `json:"nic" validate:"nonzero"`
	SystemCloudInit json.RawMessage `json:"systemCloudInit" validate:"nonzero"`
	UserCloudInit   json.RawMessage `json:"userCloudInit" validate:"nonzero"`
}

func (s VMCreate) Validate() error {

	return validator.Validate(s)
}
