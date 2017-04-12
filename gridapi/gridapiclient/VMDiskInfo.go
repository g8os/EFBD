package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type VMDiskInfo struct {
	ReadIops        int `json:"readIops" validate:"nonzero"`
	ReadThroughput  int `json:"readThroughput" validate:"nonzero"`
	WriteIops       int `json:"writeIops" validate:"nonzero"`
	WriteThroughput int `json:"writeThroughput" validate:"nonzero"`
}

func (s VMDiskInfo) Validate() error {

	return validator.Validate(s)
}
