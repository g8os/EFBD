package gridapi

type EnumVMStatus string

const (
	EnumVMStatusrunning   EnumVMStatus = "running"
	EnumVMStatushalted    EnumVMStatus = "halted"
	EnumVMStatuspaused    EnumVMStatus = "paused"
	EnumVMStatushalting   EnumVMStatus = "halting"
	EnumVMStatusmigrating EnumVMStatus = "migrating"
)
