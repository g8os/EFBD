package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type GithubAccount struct {
	Avatar_url string `json:"avatar_url" validate:"nonzero"`
	Html_url   string `json:"html_url" validate:"nonzero"`
	Id         int    `json:"id" validate:"nonzero"`
	Login      string `json:"login" validate:"nonzero"`
	Name       string `json:"name" validate:"nonzero"`
}

func (s GithubAccount) Validate() error {

	return validator.Validate(s)
}
