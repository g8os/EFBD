package user

import (
	"gopkg.in/validator.v2"
	"regexp"
)

type Address struct {
	City       string `json:"city" validate:"max=30"`
	Country    string `json:"country" validate:"max=40"`
	Nr         string `json:"nr" validate:"max=10"`
	Other      string `json:"other" validate:"max=30"`
	Postalcode string `json:"postalcode" validate:"max=20"`
	Street     string `json:"street" validate:"max=50"`
	Label      string `json:"label" validate:"regexp=^[a-zA-Z\d\-_\s]{2,50}$"`
}

func (a Address) Validate() bool {
	return validator.Validate(a) == nil && regexp.MustCompile(`^[a-zA-Z\d\-_\s]{2,50}$`).MatchString(a.Label)
}
