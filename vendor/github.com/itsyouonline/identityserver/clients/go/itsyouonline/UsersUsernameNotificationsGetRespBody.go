package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type UsersUsernameNotificationsGetRespBody struct {
	Approvals        []JoinOrganizationInvitation `json:"approvals" validate:"nonzero"`
	ContractRequests []ContractSigningRequest     `json:"contractRequests" validate:"nonzero"`
	Invitations      []JoinOrganizationInvitation `json:"invitations" validate:"nonzero"`
	Missingscopes    []MissingScopes              `json:"missingscopes" validate:"nonzero"`
}

func (s UsersUsernameNotificationsGetRespBody) Validate() error {

	return validator.Validate(s)
}
