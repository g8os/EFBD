package itsyouonline

import (
	"gopkg.in/validator.v2"
)

// For an explanation about scopes and scopemapping, see https://github.com/itsyouonline/identityserver/blob/master/docs/oauth2/scopes.md
type Authorization struct {
	Addresses      []AuthorizationMap `json:"addresses,omitempty"`
	Bankaccounts   []AuthorizationMap `json:"bankaccounts,omitempty"`
	Emailaddresses []AuthorizationMap `json:"emailaddresses,omitempty"`
	Facebook       bool               `json:"facebook,omitempty"`
	Github         bool               `json:"github,omitempty"`
	GrantedTo      string             `json:"grantedTo" validate:"nonzero"`
	Organizations  []string           `json:"organizations" validate:"nonzero"`
	Phonenumbers   []AuthorizationMap `json:"phonenumbers,omitempty"`
	PublicKeys     []AuthorizationMap `json:"publicKeys,omitempty"`
	Username       string             `json:"username" validate:"nonzero"`
}

func (s Authorization) Validate() error {

	return validator.Validate(s)
}
