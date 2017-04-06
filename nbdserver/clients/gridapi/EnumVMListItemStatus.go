package gridapi

type EnumVMListItemStatus string

const (
	EnumVMListItemStatusrunning EnumVMListItemStatus = "running"
	EnumVMListItemStatushalted  EnumVMListItemStatus = "halted"
	EnumVMListItemStatuspaused  EnumVMListItemStatus = "paused"
)
