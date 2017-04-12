package gridapiclient

type EnumDiskInfoType string

const (
	EnumDiskInfoTypessd     EnumDiskInfoType = "ssd"
	EnumDiskInfoTypenvme    EnumDiskInfoType = "nvme"
	EnumDiskInfoTypehdd     EnumDiskInfoType = "hdd"
	EnumDiskInfoTypearchive EnumDiskInfoType = "archive"
)
