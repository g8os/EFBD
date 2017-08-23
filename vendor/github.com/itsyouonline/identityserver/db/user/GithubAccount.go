package user

type GithubAccount struct {
	Login      string `json:"login"`
	Id         int    `json:"id"`
	Avatar_url string `json:"avatar_url"`
	Html_url   string `json:"html_url"`
	Name       string `json:"name"`
}
