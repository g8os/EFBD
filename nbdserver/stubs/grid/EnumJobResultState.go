package grid

type EnumJobResultState string

const (
	EnumJobResultStateunknown_cmd  EnumJobResultState = "unknown_cmd"
	EnumJobResultStateerror        EnumJobResultState = "error"
	EnumJobResultStatesuccess      EnumJobResultState = "success"
	EnumJobResultStatekilled       EnumJobResultState = "killed"
	EnumJobResultStatetimeout      EnumJobResultState = "timeout"
	EnumJobResultStateduplicate_id EnumJobResultState = "duplicate_id"
	EnumJobResultStaterunning      EnumJobResultState = "running"
)
