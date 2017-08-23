package user

type FacebookAccount struct {
	Id      string `json:"id"`
	Picture string `json:"picture"`
	Link    string `json:"link"`
	Name    string `json:"name"`
}
