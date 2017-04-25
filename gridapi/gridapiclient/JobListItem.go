package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// Result object of a job
type JobListItem struct {
	Id        string `json:"id" validate:"nonzero"`
	StartTime int64  `json:"startTime" validate:"nonzero"`
}

func (s JobListItem) Validate() error {

	return validator.Validate(s)
}
