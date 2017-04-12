package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// Result of a core.state command
type CoreStateResult struct {
	Cpu  int `json:"cpu" validate:"nonzero"`
	Rss  int `json:"rss" validate:"nonzero"`
	Swap int `json:"swap" validate:"nonzero"`
	Vms  int `json:"vms" validate:"nonzero"`
}

func (s CoreStateResult) Validate() error {

	return validator.Validate(s)
}
