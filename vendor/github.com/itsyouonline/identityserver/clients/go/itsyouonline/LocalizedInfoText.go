package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type LocalizedInfoText struct {
	Langkey string `json:"langkey" validate:"nonzero"`
	Text    string `json:"text" validate:"nonzero"`
}

func (s LocalizedInfoText) Validate() error {

	return validator.Validate(s)
}
