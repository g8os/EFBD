package organization

// LocalizedInfoText is a key-value pair that binds a (translated) text to a language
type LocalizedInfoText struct {
	LangKey string `json:"langkey"`
	Text    string `json:"text"`
}
