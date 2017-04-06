package grid

type EnumStoragePoolListItemStatus string

const (
	EnumStoragePoolListItemStatushealthy  EnumStoragePoolListItemStatus = "healthy"
	EnumStoragePoolListItemStatusdegraded EnumStoragePoolListItemStatus = "degraded"
	EnumStoragePoolListItemStatuserror    EnumStoragePoolListItemStatus = "error"
)
