package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// Result object of a job
type JobResult struct {
	Data      string             `json:"data" validate:"nonzero"`
	Id        string             `json:"id" validate:"nonzero"`
	Level     int                `json:"level" validate:"nonzero"`
	Name      EnumJobResultName  `json:"name" validate:"nonzero"`
	StartTime int64              `json:"startTime" validate:"nonzero"`
	State     EnumJobResultState `json:"state" validate:"nonzero"`
	Stderr    string             `json:"stderr" validate:"nonzero"`
	Stdout    string             `json:"stdout" validate:"nonzero"`
}

func (s JobResult) Validate() error {

	return validator.Validate(s)
}
