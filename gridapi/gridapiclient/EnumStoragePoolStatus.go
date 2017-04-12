package gridapiclient

type EnumStoragePoolStatus string

const (
	EnumStoragePoolStatushealthy  EnumStoragePoolStatus = "healthy"
	EnumStoragePoolStatusdegraded EnumStoragePoolStatus = "degraded"
	EnumStoragePoolStatuserror    EnumStoragePoolStatus = "error"
)
