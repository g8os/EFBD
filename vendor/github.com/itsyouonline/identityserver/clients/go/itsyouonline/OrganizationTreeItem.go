package itsyouonline

import (
	"gopkg.in/validator.v2"
)

type OrganizationTreeItem struct {
	Children []OrganizationTreeItem `json:"children" validate:"nonzero"`
	Globalid string                 `json:"globalid" validate:"nonzero"`
}

func (s OrganizationTreeItem) Validate() error {

	return validator.Validate(s)
}
