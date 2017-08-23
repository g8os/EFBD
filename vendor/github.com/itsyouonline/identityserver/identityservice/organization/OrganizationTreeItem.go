package organization

type OrganizationTreeItem struct {
	Children []*OrganizationTreeItem `json:"children"`
	GlobalID string                  `json:"globalid"`
}
