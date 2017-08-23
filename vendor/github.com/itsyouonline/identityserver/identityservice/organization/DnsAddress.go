package organization

import (
	"gopkg.in/validator.v2"
	"regexp"
)

type DnsAddress struct {
	Name string `json:"name" validate:"min=4,max=250,nonzero,regexp=^[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9](?:\.[a-zA-Z]{2,})+$"`
}

func (d DnsAddress) Validate() bool {
	return validator.Validate(d) == nil &&
		regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9](?:\.[a-zA-Z]{2,})+$`).MatchString(d.Name)
}
