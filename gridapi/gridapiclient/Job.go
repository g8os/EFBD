package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// Job that is be executed in the Node/Container. More information can be found here https://github.com/g8os/node/blob/master/docs/commands.md
type Job struct {
	Id              string `json:"id" validate:"nonzero"`
	LogLevels       []int  `json:"logLevels" validate:"nonzero"`
	MaxRestart      int    `json:"maxRestart" validate:"nonzero"`
	MaxTime         int    `json:"maxTime" validate:"nonzero"`
	Queue           string `json:"queue" validate:"nonzero"`
	RecurringPeriod int    `json:"recurringPeriod" validate:"nonzero"`
	StatsInterval   int    `json:"statsInterval" validate:"nonzero"`
	Tags            string `json:"tags" validate:"nonzero"`
}

func (s Job) Validate() error {

	return validator.Validate(s)
}
