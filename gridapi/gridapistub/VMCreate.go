package gridapistub

import (
	"encoding/json"
	"gopkg.in/validator.v2"
)

type VMCreate struct {
	Cpu             int             `json:"cpu" validate:"nonzero"`
	Disks           []string        `json:"disks" validate:"nonzero"`
	Id              string          `json:"id" validate:"nonzero"`
	Memory          int             `json:"memory" validate:"nonzero"`
	Nics            []NicLink       `json:"nics" validate:"nonzero"`
	SystemCloudInit json.RawMessage `json:"systemCloudInit" validate:"nonzero"`
	UserCloudInit   json.RawMessage `json:"userCloudInit" validate:"nonzero"`
}

func (s VMCreate) Validate() error {

	return validator.Validate(s)
}
