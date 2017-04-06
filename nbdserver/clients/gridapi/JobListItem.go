package gridapi

import (
	"gopkg.in/validator.v2"
)

// Result object of a job
type JobListItem struct {
	Id        string `json:"id" validate:"nonzero"`
	Starttime int    `json:"starttime" validate:"nonzero"`
}

func (s JobListItem) Validate() error {

	return validator.Validate(s)
}
