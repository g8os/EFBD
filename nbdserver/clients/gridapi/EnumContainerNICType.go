package gridapi

type EnumContainerNICType string

const (
	EnumContainerNICTypezerotier EnumContainerNICType = "zerotier"
	EnumContainerNICTypevxlan    EnumContainerNICType = "vxlan"
	EnumContainerNICTypevlan     EnumContainerNICType = "vlan"
	EnumContainerNICTypedefault  EnumContainerNICType = "default"
)
