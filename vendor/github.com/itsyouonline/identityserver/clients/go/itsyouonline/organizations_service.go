package itsyouonline

import (
	"encoding/json"
	"net/http"
)

type OrganizationsService service

// Get the 2FA validity time for the organization, in seconds
func (s *OrganizationsService) Get2faValidityTime(globalid string, headers, queryParams map[string]interface{}) (int, *http.Response, error) {
	var u int

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/2fa/validity", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update the 2FA validity time for the organization
func (s *OrganizationsService) Set2faValidityTime(globalid string, body int, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/organizations/"+globalid+"/2fa/validity", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Removes an API key
func (s *OrganizationsService) DeleteOrganizationAPIKey(label, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/apikeys/"+label, headers, queryParams)
}

// Get an api key from an organization
func (s *OrganizationsService) GetOrganizationAPIKey(label, globalid string, headers, queryParams map[string]interface{}) (OrganizationAPIKey, *http.Response, error) {
	var u OrganizationAPIKey

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/apikeys/"+label, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Updates the label or other properties of a key.
func (s *OrganizationsService) UpdateOrganizationAPIKey(label, globalid string, body OrganizationsGlobalidApikeysLabelPutReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/organizations/"+globalid+"/apikeys/"+label, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get the list of active api keys.
func (s *OrganizationsService) GetOrganizationAPIKeyLabels(globalid string, headers, queryParams map[string]interface{}) ([]string, *http.Response, error) {
	var u []string

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/apikeys", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new API Key, a secret itself should not be provided, it will be generated serverside.
func (s *OrganizationsService) CreateNewOrganizationAPIKey(globalid string, body OrganizationAPIKey, headers, queryParams map[string]interface{}) (OrganizationAPIKey, *http.Response, error) {
	var u OrganizationAPIKey

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/apikeys", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get the contracts where the organization is 1 of the parties. Order descending by date.
func (s *OrganizationsService) GetOrganizationContracts(globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/contracts", headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Create a new contract.
func (s *OrganizationsService) CreateOrganizationContracty(globalid string, body Contract, headers, queryParams map[string]interface{}) (Contract, *http.Response, error) {
	var u Contract

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/contracts", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get the description for an organization for this langkey, try to use the English is there is no description for this langkey
func (s *OrganizationsService) GetDescriptionWithFallback(langkey, globalid string, headers, queryParams map[string]interface{}) (LocalizedInfoText, *http.Response, error) {
	var u LocalizedInfoText

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/description/"+langkey+"/withfallback", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete the description for this organization for a given language key
func (s *OrganizationsService) DeleteDescription(langkey, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/description/"+langkey, headers, queryParams)
}

// Get the description for an organization for this langkey
func (s *OrganizationsService) GetDescription(langkey, globalid string, headers, queryParams map[string]interface{}) (LocalizedInfoText, *http.Response, error) {
	var u LocalizedInfoText

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/description/"+langkey, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Set the description for this organization for a given language key
func (s *OrganizationsService) SetDescription(globalid string, body LocalizedInfoText, headers, queryParams map[string]interface{}) (LocalizedInfoText, *http.Response, error) {
	var u LocalizedInfoText

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/description", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update the description for this organization for a given language key
func (s *OrganizationsService) UpdateDescription(globalid string, body LocalizedInfoText, headers, queryParams map[string]interface{}) (LocalizedInfoText, *http.Response, error) {
	var u LocalizedInfoText

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/organizations/"+globalid+"/description", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Removes a DNS name associated with an organization
func (s *OrganizationsService) DeleteOrganizationDns(dnsname, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/dns/"+dnsname, headers, queryParams)
}

// Updates an existing DNS name associated with an organization
func (s *OrganizationsService) UpdateOrganizationDns(dnsname, globalid string, body DnsAddress, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/organizations/"+globalid+"/dns/"+dnsname, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Creates a new DNS name associated with an organization
func (s *OrganizationsService) CreateOrganizationDns(globalid string, body DnsAddress, headers, queryParams map[string]interface{}) (DnsAddress, *http.Response, error) {
	var u DnsAddress

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/dns", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Cancel a pending invitation.
func (s *OrganizationsService) RemovePendingOrganizationInvitation(username, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/invitations/"+username, headers, queryParams)
}

// Get the list of pending invitations for users to join this organization.
func (s *OrganizationsService) GetInvitations(globalid string, headers, queryParams map[string]interface{}) ([]JoinOrganizationInvitation, *http.Response, error) {
	var u []JoinOrganizationInvitation

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/invitations", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Checks if the user has memberschip rights on the organization
func (s *OrganizationsService) UserIsMember(username, globalid string, headers, queryParams map[string]interface{}) (OrganizationsGlobalidIsmemberUsernameGetRespBody, *http.Response, error) {
	var u OrganizationsGlobalidIsmemberUsernameGetRespBody

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/ismember/"+username, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Removes the Logo from an organization
func (s *OrganizationsService) DeleteOrganizationLogo(globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/logo", headers, queryParams)
}

// Get the Logo from an organization
func (s *OrganizationsService) GetOrganizationLogo(globalid string, headers, queryParams map[string]interface{}) (string, *http.Response, error) {
	var u string

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/logo", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Set the organization Logo for the organization
func (s *OrganizationsService) SetOrganizationLogo(globalid string, body OrganizationsGlobalidLogoPutReqBody, headers, queryParams map[string]interface{}) (string, *http.Response, error) {
	var u string

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/organizations/"+globalid+"/logo", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Remove a member from an organization.
func (s *OrganizationsService) RemoveOrganizationMember(username, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/members/"+username, headers, queryParams)
}

// Invite someone to become member of an organization.
func (s *OrganizationsService) AddOrganizationMember(globalid string, body OrganizationsGlobalidMembersPostReqBody, headers, queryParams map[string]interface{}) (JoinOrganizationInvitation, *http.Response, error) {
	var u JoinOrganizationInvitation

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/members", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update an organization membership
func (s *OrganizationsService) UpdateOrganizationMemberShip(globalid string, body Membership, headers, queryParams map[string]interface{}) (Organization, *http.Response, error) {
	var u Organization

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/organizations/"+globalid+"/members", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Reject the invite for one of your organizations
func (s *OrganizationsService) RejectOrganizationInvite(invitingorg, role, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/organizations/"+invitingorg+"/roles/"+role, headers, queryParams)
}

// Accept the invite for one of your organizations
func (s *OrganizationsService) AcceptOrganizationInvite(invitingorg, role, globalid string, body JoinOrganizationInvitation, headers, queryParams map[string]interface{}) (JoinOrganizationInvitation, *http.Response, error) {
	var u JoinOrganizationInvitation

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/organizations/"+invitingorg+"/roles/"+role, &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Remove an orgmember or orgowner organization to the includesuborgsof list
func (s *OrganizationsService) RemoveIncludeSubOrgsOf(orgmember, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/orgmembers/includesuborgs/"+orgmember, headers, queryParams)
}

// Add an orgmember or orgowner organization to the includesuborgsof list
func (s *OrganizationsService) AddIncludeSubOrgsOf(globalid string, body OrganizationsGlobalidOrgmembersIncludesuborgsPostReqBody, headers, queryParams map[string]interface{}) (OrganizationsGlobalidOrgmembersIncludesuborgsPostRespBody, *http.Response, error) {
	var u OrganizationsGlobalidOrgmembersIncludesuborgsPostRespBody

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/orgmembers/includesuborgs", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Remove an organization as a member
func (s *OrganizationsService) DeleteOrgMember(globalid2, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/orgmembers/"+globalid2, headers, queryParams)
}

// Add another organization as a member of this one
func (s *OrganizationsService) SetOrgMember(globalid string, body OrganizationsGlobalidOrgmembersPostReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/orgmembers", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Update the membership status of an organization
func (s *OrganizationsService) UpdateOrganizationOrgMemberShip(globalid string, body OrganizationsGlobalidOrgmembersPutReqBody, headers, queryParams map[string]interface{}) (Organization, *http.Response, error) {
	var u Organization

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/organizations/"+globalid+"/orgmembers", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Remove an organization as an owner
func (s *OrganizationsService) DeleteOrgOwner(globalid2, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/orgowners/"+globalid2, headers, queryParams)
}

// Add another organization as an owner of this one
func (s *OrganizationsService) SetOrgOwner(globalid string, body OrganizationsGlobalidOrgownersPostReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/orgowners", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Remove an owner from organization
func (s *OrganizationsService) RemoveOrganizationOwner(username, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/owners/"+username, headers, queryParams)
}

// Invite someone to become owner of an organization.
func (s *OrganizationsService) AddOrganizationOwner(globalid string, body Member, headers, queryParams map[string]interface{}) (JoinOrganizationInvitation, *http.Response, error) {
	var u JoinOrganizationInvitation

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/owners", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Removes a RegistryEntry from the organization's registry
func (s *OrganizationsService) DeleteOrganizationRegistryEntry(key, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/registry/"+key, headers, queryParams)
}

// Get a RegistryEntry from the organization's registry.
func (s *OrganizationsService) GetOrganizationRegistryEntry(key, globalid string, headers, queryParams map[string]interface{}) (RegistryEntry, *http.Response, error) {
	var u RegistryEntry

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/registry/"+key, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Lists the RegistryEntries in an organization's registry.
func (s *OrganizationsService) ListOrganizationRegistry(globalid string, headers, queryParams map[string]interface{}) ([]RegistryEntry, *http.Response, error) {
	var u []RegistryEntry

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/registry", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Adds a RegistryEntry to the organization's registry, if the key is already used, it is overwritten.
func (s *OrganizationsService) AddOrganizationRegistryEntry(globalid string, body RegistryEntry, headers, queryParams map[string]interface{}) (RegistryEntry, *http.Response, error) {
	var u RegistryEntry

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/registry", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Deletes a required scope
func (s *OrganizationsService) DeleteRequiredScope(requiredscope, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid+"/requiredscopes/"+requiredscope, headers, queryParams)
}

// Updates a required scope
func (s *OrganizationsService) UpdateRequiredScope(requiredscope, globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/organizations/"+globalid+"/requiredscopes/"+requiredscope, nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Adds a required scope
func (s *OrganizationsService) AddRequiredScope(globalid string, body RequiredScope, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/requiredscopes", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Transfer a suborganization from one parent to another
func (s *OrganizationsService) TransferSubOrganization(globalid string, body OrganizationsGlobalidTransfersuborganizationPostReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid+"/transfersuborganization", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Tree structure of all suborganizations
func (s *OrganizationsService) GetOrganizationTree(globalid string, headers, queryParams map[string]interface{}) ([]OrganizationTreeItem, *http.Response, error) {
	var u []OrganizationTreeItem

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/tree", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get all users from this organization, not including suborganizations.
func (s *OrganizationsService) GetOrganizationUsers(globalid string, headers, queryParams map[string]interface{}) (GetOrganizationUsersResponseBody, *http.Response, error) {
	var u GetOrganizationUsersResponseBody

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid+"/users", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Deletes an organization and all data linked to it (join-organization-invitations, oauth_access_tokens, oauth_clients, logo)
func (s *OrganizationsService) DeleteOrganization(globalid string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/organizations/"+globalid, headers, queryParams)
}

// Get organization info
func (s *OrganizationsService) GetOrganization(globalid string, headers, queryParams map[string]interface{}) (Organization, *http.Response, error) {
	var u Organization

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/organizations/"+globalid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new suborganization.
func (s *OrganizationsService) CreateNewSubOrganization(globalid string, body Organization, headers, queryParams map[string]interface{}) (Organization, *http.Response, error) {
	var u Organization

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations/"+globalid, &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new organization. 1 user should be in the owners list. Validation is performed to check if the securityScheme allows management on this user.
func (s *OrganizationsService) CreateNewOrganization(body Organization, headers, queryParams map[string]interface{}) (Organization, *http.Response, error) {
	var u Organization

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/organizations", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}
