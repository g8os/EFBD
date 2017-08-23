package user

import (
	"gopkg.in/validator.v2"
	"regexp"
)

//Phonenumber defines a phonenumber and has functions for validation
type Phonenumber struct {
	Label       string `json:"label" validate:"regexp=^[a-zA-Z\d\-_\s]{2,50}$"`
	Phonenumber string `json:"phonenumber" validate:"regexp=\+[0-9]{6,50}$"`
}

//Validate checks if a phone number is in a valid format
func (p Phonenumber) Validate() bool {
	return validator.Validate(p) == nil &&
		regexp.MustCompile(`^[a-zA-Z\d\-_\s]{2,50}$`).MatchString(p.Label) &&
		regexp.MustCompile(`\+[0-9]{6,50}`).MatchString(p.Phonenumber)
}
