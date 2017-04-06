package grid

type EnumClusterCreateDriveType string

const (
	EnumClusterCreateDriveTypenvme    EnumClusterCreateDriveType = "nvme"
	EnumClusterCreateDriveTypessd     EnumClusterCreateDriveType = "ssd"
	EnumClusterCreateDriveTypehdd     EnumClusterCreateDriveType = "hdd"
	EnumClusterCreateDriveTypearchive EnumClusterCreateDriveType = "archive"
)
