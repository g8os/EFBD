package gridapi

type EnumVolumeVolumetype string

const (
	EnumVolumeVolumetypeboot  EnumVolumeVolumetype = "boot"
	EnumVolumeVolumetypedb    EnumVolumeVolumetype = "db"
	EnumVolumeVolumetypecache EnumVolumeVolumetype = "cache"
	EnumVolumeVolumetypetmp   EnumVolumeVolumetype = "tmp"
)
