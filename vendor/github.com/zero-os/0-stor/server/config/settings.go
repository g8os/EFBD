package config

var (
	ServerTypeRest = "rest"
	ServerTypeGrpc = "grpc"
)

type Settings struct {
	DebugLog    bool   `json:"debug"`
	BindAddress string `json:"bind"`

	DB struct {
		Dirs struct {
			Meta string `json:"meta"`
			Data string `json:"data"`
		} `json:"dirs"`
	} `json:"db"`

	ServerType string `json:"server_type"`
}
