package itsyouonline

import (
	"github.com/itsyouonline/identityserver/clients/go/itsyouonline/goraml"
	"gopkg.in/validator.v2"
)

type JoinOrganizationInvitation struct {
	Created        goraml.DateTime                      `json:"created,omitempty"`
	Emailaddress   string                               `json:"emailaddress" validate:"nonzero"`
	Isorganization bool                                 `json:"isorganization"`
	Method         EnumJoinOrganizationInvitationMethod `json:"method" validate:"nonzero"`
	Organization   string                               `json:"organization" validate:"nonzero"`
	Phonenumber    string                               `json:"phonenumber" validate:"nonzero"`
	Role           EnumJoinOrganizationInvitationRole   `json:"role" validate:"nonzero"`
	Status         EnumJoinOrganizationInvitationStatus `json:"status" validate:"nonzero"`
	User           string                               `json:"user" validate:"nonzero"`
}

func (s JoinOrganizationInvitation) Validate() error {

	return validator.Validate(s)
}
