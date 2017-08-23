package user

type JoinOrganizationRequest struct {
	Organization string   `json:"organization"`
	Role         []string `json:"role"`
	User         string   `json:"user"`
}
