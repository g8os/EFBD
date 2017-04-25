package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// Result of a core.state command
type CoreStateResult struct {
	Cpu  float64 `json:"cpu" validate:"nonzero"`
	Rss  int64   `json:"rss" validate:"nonzero"`
	Swap int64   `json:"swap" validate:"nonzero"`
	Vms  int64   `json:"vms" validate:"nonzero"`
}

func (s CoreStateResult) Validate() error {

	return validator.Validate(s)
}
