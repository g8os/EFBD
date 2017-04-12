package gridapistub

type EnumVMListItemStatus string

const (
	EnumVMListItemStatusrunning EnumVMListItemStatus = "running"
	EnumVMListItemStatushalted  EnumVMListItemStatus = "halted"
	EnumVMListItemStatuspaused  EnumVMListItemStatus = "paused"
)
