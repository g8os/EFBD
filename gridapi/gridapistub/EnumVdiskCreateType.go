package gridapistub

type EnumVdiskCreateType string

const (
	EnumVdiskCreateTypeboot  EnumVdiskCreateType = "boot"
	EnumVdiskCreateTypedb    EnumVdiskCreateType = "db"
	EnumVdiskCreateTypecache EnumVdiskCreateType = "cache"
	EnumVdiskCreateTypetmp   EnumVdiskCreateType = "tmp"
)
