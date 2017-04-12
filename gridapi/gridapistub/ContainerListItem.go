package gridapistub

import (
	"gopkg.in/validator.v2"
)

type ContainerListItem struct {
	Flist    string                      `json:"flist" validate:"nonzero"`
	Hostname string                      `json:"hostname" validate:"nonzero"`
	Id       string                      `json:"id" validate:"nonzero"`
	Status   EnumContainerListItemStatus `json:"status" validate:"nonzero"`
}

func (s ContainerListItem) Validate() error {

	return validator.Validate(s)
}
