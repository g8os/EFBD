package user

// Models used when requesting / parsing information from facebook

type FacebookPic struct {
	Is_silhouette bool
	Url           string
}

type FacebookDataPic struct {
	Data FacebookPic
}

type FBInfo struct {
	Id      string
	Picture FacebookDataPic
	Link    string
	Name    string
}

type FacebookError struct {
	Message    string
	Type       string
	Code       int
	Fbtrace_id string
}
