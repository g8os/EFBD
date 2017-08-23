package organization

// OrganizationInfoText stores all the (translations of) the information text on the signin/signup page for an given organization
type OrganizationInfoText struct {
	Globalid  string              `json:"globalid"`
	InfoTexts []LocalizedInfoText `json:"infotexts"`
}
