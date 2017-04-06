package grid

import (
	"gopkg.in/validator.v2"
)

type VMNetInfo struct {
	ReceivedPackets       int `json:"receivedPackets" validate:"nonzero"`
	ReceivedThroughput    int `json:"receivedThroughput" validate:"nonzero"`
	TransmittedPackets    int `json:"transmittedPackets" validate:"nonzero"`
	TransmittedThroughput int `json:"transmittedThroughput" validate:"nonzero"`
}

func (s VMNetInfo) Validate() error {

	return validator.Validate(s)
}
