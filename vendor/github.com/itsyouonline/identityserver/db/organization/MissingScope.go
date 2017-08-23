package organization

type MissingScope struct {
	Organization string   `json:"organization"`
	Scopes       []string `json:"scopes"`
}
