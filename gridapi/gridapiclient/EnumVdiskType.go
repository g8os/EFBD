package gridapiclient

type EnumVdiskType string

const (
	EnumVdiskTypeboot  EnumVdiskType = "boot"
	EnumVdiskTypedb    EnumVdiskType = "db"
	EnumVdiskTypecache EnumVdiskType = "cache"
	EnumVdiskTypetmp   EnumVdiskType = "tmp"
)
