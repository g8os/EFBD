package grid

type EnumVolumeCreateVolumetype string

const (
	EnumVolumeCreateVolumetypeboot  EnumVolumeCreateVolumetype = "boot"
	EnumVolumeCreateVolumetypedb    EnumVolumeCreateVolumetype = "db"
	EnumVolumeCreateVolumetypecache EnumVolumeCreateVolumetype = "cache"
	EnumVolumeCreateVolumetypetmp   EnumVolumeCreateVolumetype = "tmp"
)
