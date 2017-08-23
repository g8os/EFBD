package organization

type searchMember struct {
	SearchString string `json:"searchstring"`
}

type Membership struct {
	Username string `json:"username"`
	Role     string `json:"role"`
}
