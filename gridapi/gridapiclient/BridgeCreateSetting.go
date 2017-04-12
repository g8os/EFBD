package gridapiclient

import (
	"gopkg.in/validator.v2"
)

// Networking settings, depending on the selected mode.
//   none:
//     no settings, bridge won't get any ip settings
//   static:
//     settings={'cidr': 'ip/net'}
//     bridge will get assigned the given IP address
//   dnsmasq:
//     settings={'cidr': 'ip/net', 'start': 'ip', 'end': 'ip'}
//     bridge will get assigned the ip in cidr
//     and each running container that is attached to this IP will get
//     IP from the start/end range. Netmask of the range is the netmask
//     part of the provided cidr.
//     if nat is true, SNAT rules will be automatically added in the firewall.
type BridgeCreateSetting struct {
	Cidr  string `json:"cidr" validate:"nonzero"`
	End   string `json:"end" validate:"nonzero"`
	Start string `json:"start" validate:"nonzero"`
}

func (s BridgeCreateSetting) Validate() error {

	return validator.Validate(s)
}
