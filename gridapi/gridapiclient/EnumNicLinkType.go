package gridapiclient

type EnumNicLinkType string

const (
	EnumNicLinkTypevlan    EnumNicLinkType = "vlan"
	EnumNicLinkTypevxlan   EnumNicLinkType = "vxlan"
	EnumNicLinkTypedefault EnumNicLinkType = "default"
)