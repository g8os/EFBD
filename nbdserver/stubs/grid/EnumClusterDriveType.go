package grid

type EnumClusterDriveType string

const (
	EnumClusterDriveTypenvme    EnumClusterDriveType = "nvme"
	EnumClusterDriveTypessd     EnumClusterDriveType = "ssd"
	EnumClusterDriveTypehdd     EnumClusterDriveType = "hdd"
	EnumClusterDriveTypearchive EnumClusterDriveType = "archive"
)
