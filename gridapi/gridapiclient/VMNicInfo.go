package gridapiclient

import (
	"gopkg.in/validator.v2"
)

type VMNicInfo struct {
	ReceivedPackets       int `json:"receivedPackets" validate:"nonzero"`
	ReceivedThroughput    int `json:"receivedThroughput" validate:"nonzero"`
	TransmittedPackets    int `json:"transmittedPackets" validate:"nonzero"`
	TransmittedThroughput int `json:"transmittedThroughput" validate:"nonzero"`
}

func (s VMNicInfo) Validate() error {

	return validator.Validate(s)
}
