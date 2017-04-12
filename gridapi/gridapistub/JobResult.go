package gridapistub

import (
	"gopkg.in/validator.v2"
)

// Result object of a job
type JobResult struct {
	Data      string             `json:"data" validate:"nonzero"`
	Id        string             `json:"id" validate:"nonzero"`
	Level     string             `json:"level" validate:"nonzero"`
	Name      EnumJobResultName  `json:"name" validate:"nonzero"`
	Starttime int                `json:"starttime" validate:"nonzero"`
	State     EnumJobResultState `json:"state" validate:"nonzero"`
	Stderr    string             `json:"stderr" validate:"nonzero"`
	Stdout    string             `json:"stdout" validate:"nonzero"`
}

func (s JobResult) Validate() error {

	return validator.Validate(s)
}
