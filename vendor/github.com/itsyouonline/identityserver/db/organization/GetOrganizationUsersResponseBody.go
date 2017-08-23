package organization

type GetOrganizationUsersResponseBody struct {
	HasEditPermissions bool               `json:"haseditpermissions"`
	Users              []OrganizationUser `json:"users"`
}
