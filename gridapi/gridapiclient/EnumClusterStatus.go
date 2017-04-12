package gridapiclient

type EnumClusterStatus string

const (
	EnumClusterStatusempty     EnumClusterStatus = "empty"
	EnumClusterStatusdeploying EnumClusterStatus = "deploying"
	EnumClusterStatusready     EnumClusterStatus = "ready"
	EnumClusterStatuserror     EnumClusterStatus = "error"
)
