package gridapistub

type EnumVdiskStatus string

const (
	EnumVdiskStatusrunning     EnumVdiskStatus = "running"
	EnumVdiskStatushalted      EnumVdiskStatus = "halted"
	EnumVdiskStatusrollingback EnumVdiskStatus = "rollingback"
)
