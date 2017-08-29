package itsyouonline

import (
	"encoding/json"
	"net/http"
)

type UsersService service

// Get the avatar file associated with this id
func (s *UsersService) GetAvatarImage(hash string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/avatar/img/"+hash, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Removes an address
func (s *UsersService) DeleteUserAddress(label, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/addresses/"+label, headers, queryParams)
}

// Get the details of an address.
func (s *UsersService) GetUserAddressByLabel(label, username string, headers, queryParams map[string]interface{}) (Address, *http.Response, error) {
	var u Address

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/addresses/"+label, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update the label and/or value of an existing address.
func (s *UsersService) UpdateUserAddress(label, username string, body Address, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/addresses/"+label, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// List of all of the user his email addresses.
func (s *UsersService) GetUserAddresses(username string, headers, queryParams map[string]interface{}) ([]Address, *http.Response, error) {
	var u []Address

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/addresses", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Register a new address
func (s *UsersService) RegisterNewUserAddress(username string, body Address, headers, queryParams map[string]interface{}) (Address, *http.Response, error) {
	var u Address

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/addresses", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Removes an API key
func (s *UsersService) DeleteAPIkey(label, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/apikeys/"+label, headers, queryParams)
}

// Get an API key by label
func (s *UsersService) GetAPIkey(label, username string, headers, queryParams map[string]interface{}) (UserAPIKey, *http.Response, error) {
	var u UserAPIKey

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/apikeys/"+label, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Updates the label for the API key
func (s *UsersService) UpdateAPIkey(label, username string, body UsersUsernameApikeysLabelPutReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/apikeys/"+label, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Lists the API keys
func (s *UsersService) ListAPIKeys(username string, headers, queryParams map[string]interface{}) ([]UserAPIKey, *http.Response, error) {
	var u []UserAPIKey

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/apikeys", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Adds an APIKey to the user
func (s *UsersService) AddApiKey(username string, body UsersUsernameApikeysPostReqBody, headers, queryParams map[string]interface{}) (UserAPIKey, *http.Response, error) {
	var u UserAPIKey

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/apikeys", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Remove the authorization for an organization, the granted organization will no longer have access the user's information.
func (s *UsersService) DeleteAuthorization(grantedTo, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/authorizations/"+grantedTo, headers, queryParams)
}

// Get the authorization for a specific organization.
func (s *UsersService) GetAuthorization(grantedTo, username string, headers, queryParams map[string]interface{}) (Authorization, *http.Response, error) {
	var u Authorization

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/authorizations/"+grantedTo, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Modify which information an organization is able to see.
func (s *UsersService) UpdateAuthorization(grantedTo, username string, body Authorization, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/authorizations/"+grantedTo, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get the list of authorizations.
func (s *UsersService) GetAllAuthorizations(username string, headers, queryParams map[string]interface{}) ([]Authorization, *http.Response, error) {
	var u []Authorization

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/authorizations", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new avatar with the specified label from a provided image file
func (s *UsersService) CreateAvatarFromImage(label, username string, headers, queryParams map[string]interface{}) (Avatar, *http.Response, error) {
	var u Avatar

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/avatar/img/"+label, nil, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update the avatar and possibly the avatar file stored on itsyou.online
func (s *UsersService) UpdateAvatarFile(newlabel, label, username string, headers, queryParams map[string]interface{}) (Avatar, *http.Response, error) {
	var u Avatar

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/avatar/"+label+"/to/"+newlabel, nil, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete the avatar with the specified label
func (s *UsersService) DeleteAvatar(label, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/avatar/"+label, headers, queryParams)
}

// Update the avatar and possibly the link to the avatar
func (s *UsersService) UpdateAvatarLink(label, username string, body Avatar, headers, queryParams map[string]interface{}) (Avatar, *http.Response, error) {
	var u Avatar

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/avatar/"+label, &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// List all avatars for the user
func (s *UsersService) GetAvatars(username string, headers, queryParams map[string]interface{}) (Avatar, *http.Response, error) {
	var u Avatar

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/avatar", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new avatar with the specified label from a link
func (s *UsersService) CreateAvatarFromLink(username string, body Avatar, headers, queryParams map[string]interface{}) (Avatar, *http.Response, error) {
	var u Avatar

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/avatar", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete a BankAccount
func (s *UsersService) DeleteUserBankAccount(username, label string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/banks/"+label, headers, queryParams)
}

// Get the details of a bank account
func (s *UsersService) GetUserBankAccountByLabel(username, label string, headers, queryParams map[string]interface{}) (BankAccount, *http.Response, error) {
	var u BankAccount

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/banks/"+label, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update an existing bankaccount and label.
func (s *UsersService) UpdateUserBankAccount(username, label string, body BankAccount, headers, queryParams map[string]interface{}) (BankAccount, *http.Response, error) {
	var u BankAccount

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/banks/"+label, &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// List of the user his bank accounts.
func (s *UsersService) GetUserBankAccounts(username string, headers, queryParams map[string]interface{}) ([]BankAccount, *http.Response, error) {
	var u []BankAccount

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/banks", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create new bank account
func (s *UsersService) CreateUserBankAccount(username string, body UsersUsernameBanksPostReqBody, headers, queryParams map[string]interface{}) (UsersUsernameBanksPostRespBody, *http.Response, error) {
	var u UsersUsernameBanksPostRespBody

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/banks", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get the contracts where the user is 1 of the parties. Order descending by date.
func (s *UsersService) GetUserContracts(username string, headers, queryParams map[string]interface{}) ([]Contract, *http.Response, error) {
	var u []Contract

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/contracts", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new contract.
func (s *UsersService) CreateUserContract(username string, body Contract, headers, queryParams map[string]interface{}) (Contract, *http.Response, error) {
	var u Contract

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/contracts", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Removes an address
func (s *UsersService) DeleteDigitalAssetAddress(label, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/digitalwallet/"+label, headers, queryParams)
}

// Get the details of a digital wallet address.
func (s *UsersService) GetDigitalAssetAddressByLabel(label, username string, headers, queryParams map[string]interface{}) (DigitalAssetAddress, *http.Response, error) {
	var u DigitalAssetAddress

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/digitalwallet/"+label, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update the label and/or value of an existing address.
func (s *UsersService) UpdateDigitalAssetAddress(label, username string, body DigitalAssetAddress, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/digitalwallet/"+label, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// List all of the user his digital wallets.
func (s *UsersService) GetDigitalWallet(username string, headers, queryParams map[string]interface{}) ([]DigitalAssetAddress, *http.Response, error) {
	var u []DigitalAssetAddress

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/digitalwallet", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Register a new digital asset address
func (s *UsersService) RegisterNewDigitalAssetAddress(username string, body DigitalAssetAddress, headers, queryParams map[string]interface{}) (DigitalAssetAddress, *http.Response, error) {
	var u DigitalAssetAddress

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/digitalwallet", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Sends validation email to email address
func (s *UsersService) ValidateEmailAddress(label, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/emailaddresses/"+label+"/validate", nil, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Removes an email address
func (s *UsersService) DeleteEmailAddress(label, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/emailaddresses/"+label, headers, queryParams)
}

// Updates the label and/or value of an email address
func (s *UsersService) UpdateEmailAddress(label, username string, body EmailAddress, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/emailaddresses/"+label, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get a list of the user his email addresses.
func (s *UsersService) GetEmailAddresses(username string, headers, queryParams map[string]interface{}) ([]EmailAddress, *http.Response, error) {
	var u []EmailAddress

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/emailaddresses", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Register a new email address
func (s *UsersService) RegisterNewEmailAddress(username string, body EmailAddress, headers, queryParams map[string]interface{}) (EmailAddress, *http.Response, error) {
	var u EmailAddress

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/emailaddresses", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete the associated facebook account
func (s *UsersService) DeleteFacebookAccount(username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/facebook", headers, queryParams)
}

// Unlink Github Account
func (s *UsersService) DeleteGithubAccount(username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/github", headers, queryParams)
}

// Get all of the user his information. This will be limited to the scopes that the user has authorized. See https://gig.gitbooks.io/itsyouonline/content/oauth2/scopes.html and https://gig.gitbooks.io/itsyouonline/content/oauth2/availableScopes.html for more information.
func (s *UsersService) GetUserInformation(username string, headers, queryParams map[string]interface{}) (userview, *http.Response, error) {
	var u userview

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/info", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Gets the key written to this users keystore for the given label by the accessing organization
func (s *UsersService) GetKeyStoreKey(label, username string, headers, queryParams map[string]interface{}) (KeyStoreKey, *http.Response, error) {
	var u KeyStoreKey

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/keystore/"+label, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Lists all keys written to this users keystore by the accessing organization
func (s *UsersService) GetKeyStore(username string, headers, queryParams map[string]interface{}) ([]KeyStoreKey, *http.Response, error) {
	var u []KeyStoreKey

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/keystore", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Saves a new key to this users keystore. The username, globalid and timestamp will be overwritten
func (s *UsersService) SaveKeyStoreKey(username string, body KeyStoreKey, headers, queryParams map[string]interface{}) (KeyStoreKey, *http.Response, error) {
	var u KeyStoreKey

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/keystore", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update the user his firstname and lastname
func (s *UsersService) UpdateUserName(username string, body UsersUsernameNamePutReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/name", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get the list of notifications, these are pending invitations or approvals or other requests.
func (s *UsersService) GetNotifications(username string, headers, queryParams map[string]interface{}) (UsersUsernameNotificationsGetRespBody, *http.Response, error) {
	var u UsersUsernameNotificationsGetRespBody

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/notifications", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Removes the user from an organization
func (s *UsersService) LeaveOrganization(globalid, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/organizations/"+globalid+"/leave", headers, queryParams)
}

// Reject membership invitation in an organization.
func (s *UsersService) RejectMembership(globalid, role, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/organizations/"+globalid+"/roles/"+role, headers, queryParams)
}

// Accept membership in organization
func (s *UsersService) AcceptMembership(globalid, role, username string, body JoinOrganizationInvitation, headers, queryParams map[string]interface{}) (JoinOrganizationInvitation, *http.Response, error) {
	var u JoinOrganizationInvitation

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/organizations/"+globalid+"/roles/"+role, &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get the list organizations a user is owner or member of
func (s *UsersService) GetUserOrganizations(username string, headers, queryParams map[string]interface{}) (UsersUsernameOrganizationsGetRespBody, *http.Response, error) {
	var u UsersUsernameOrganizationsGetRespBody

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/organizations", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update the user his password
func (s *UsersService) UpdatePassword(username string, body UsersUsernamePasswordPutReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/password", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Sends a validation text message to the phone number.
func (s *UsersService) ValidatePhonenumber(label, username string, headers, queryParams map[string]interface{}) (UsersUsernamePhonenumbersLabelValidatePostRespBody, *http.Response, error) {
	var u UsersUsernamePhonenumbersLabelValidatePostRespBody

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/phonenumbers/"+label+"/validate", nil, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Verifies a phone number
func (s *UsersService) VerifyPhoneNumber(label, username string, body UsersUsernamePhonenumbersLabelValidatePutReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/phonenumbers/"+label+"/validate", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Removes a phonenumber
func (s *UsersService) DeleteUserPhonenumber(label, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/phonenumbers/"+label, headers, queryParams)
}

// Get the details of a phone number.
func (s *UsersService) GetUserPhonenumberByLabel(label, username string, headers, queryParams map[string]interface{}) (Phonenumber, *http.Response, error) {
	var u Phonenumber

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/phonenumbers/"+label, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Update the label and/or value of an existing phonenumber.
func (s *UsersService) UpdateUserPhonenumber(label, username string, body Phonenumber, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/phonenumbers/"+label, &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// List of all of the user his phone numbers.
func (s *UsersService) GetUserPhoneNumbers(username string, headers, queryParams map[string]interface{}) ([]Phonenumber, *http.Response, error) {
	var u []Phonenumber

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/phonenumbers", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Register a new phonenumber
func (s *UsersService) RegisterNewUserPhonenumber(username string, body Phonenumber, headers, queryParams map[string]interface{}) (Phonenumber, *http.Response, error) {
	var u Phonenumber

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/phonenumbers", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Delete a public key
func (s *UsersService) DeletePublicKey(label, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/publickeys/"+label, headers, queryParams)
}

// Get a public key
func (s *UsersService) GetPublicKey(label, username string, headers, queryParams map[string]interface{}) (PublicKey, *http.Response, error) {
	var u PublicKey

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/publickeys/"+label, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Upates the label and/or key of an existing public key
func (s *UsersService) UpdatePublicKey(label, username string, body PublicKey, headers, queryParams map[string]interface{}) (PublicKey, *http.Response, error) {
	var u PublicKey

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/publickeys/"+label, &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Lists all public keys
func (s *UsersService) ListPublicKeys(username string, headers, queryParams map[string]interface{}) ([]PublicKey, *http.Response, error) {
	var u []PublicKey

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/publickeys", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Add a public key
func (s *UsersService) AddPublicKey(username string, body PublicKey, headers, queryParams map[string]interface{}) (PublicKey, *http.Response, error) {
	var u PublicKey

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/publickeys", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Removes a RegistryEntry from the user's registry
func (s *UsersService) DeleteUserRegistryEntry(key, username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/registry/"+key, headers, queryParams)
}

// Get a RegistryEntry from the user's registry.
func (s *UsersService) GetUserRegistryEntry(key, username string, headers, queryParams map[string]interface{}) (RegistryEntry, *http.Response, error) {
	var u RegistryEntry

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/registry/"+key, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Lists the Registry entries
func (s *UsersService) ListUserRegistry(username string, headers, queryParams map[string]interface{}) ([]RegistryEntry, *http.Response, error) {
	var u []RegistryEntry

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/registry", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Adds a RegistryEntry to the user's registry, if the key is already used, it is overwritten.
func (s *UsersService) AddUserRegistryEntry(username string, body RegistryEntry, headers, queryParams map[string]interface{}) (RegistryEntry, *http.Response, error) {
	var u RegistryEntry

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/registry", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Sign a see object
func (s *UsersService) SignSeeObject(version, uniqueid, globalid, username string, body SeeView, headers, queryParams map[string]interface{}) (SeeView, *http.Response, error) {
	var u SeeView

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/see/"+uniqueid+"/"+globalid+"/sign/"+version, &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get a see object
func (s *UsersService) GetSeeObject(uniqueid, globalid, username string, headers, queryParams map[string]interface{}) (See, *http.Response, error) {
	var u See

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/see/"+uniqueid+"/"+globalid, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Updates a see object
func (s *UsersService) UpdateSeeObject(uniqueid, globalid, username string, body SeeView, headers, queryParams map[string]interface{}) (SeeView, *http.Response, error) {
	var u SeeView

	resp, err := s.client.doReqWithBody("PUT", s.client.BaseURI+"/users/"+username+"/see/"+uniqueid+"/"+globalid, &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Get a list of all see objects.
func (s *UsersService) GetSeeObjects(username string, headers, queryParams map[string]interface{}) ([]SeeView, *http.Response, error) {
	var u []SeeView

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/see", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create new see object
func (s *UsersService) CreateSeeObject(username string, body SeeView, headers, queryParams map[string]interface{}) (SeeView, *http.Response, error) {
	var u SeeView

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/see", &body, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Disable TOTP two-factor authentication.
func (s *UsersService) RemoveTOTP(username string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create request object
	return s.client.doReqNoBody("DELETE", s.client.BaseURI+"/users/"+username+"/totp", headers, queryParams)
}

// Get a TOTP secret and issuer that can be used for setting up two-factor authentication.
func (s *UsersService) GetTOTPSecret(username string, headers, queryParams map[string]interface{}) (UsersUsernameTotpGetRespBody, *http.Response, error) {
	var u UsersUsernameTotpGetRespBody

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/totp", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Enable two-factor authentication using TOTP.
func (s *UsersService) SetupTOTP(username string, body UsersUsernameTotpPostReqBody, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users/"+username+"/totp", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

// Get the possible two-factor authentication methods"
func (s *UsersService) GetTwoFAMethods(username string, headers, queryParams map[string]interface{}) (UsersUsernameTwofamethodsGetRespBody, *http.Response, error) {
	var u UsersUsernameTwofamethodsGetRespBody

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username+"/twofamethods", headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

func (s *UsersService) GetUser(username string, headers, queryParams map[string]interface{}) (User, *http.Response, error) {
	var u User

	resp, err := s.client.doReqNoBody("GET", s.client.BaseURI+"/users/"+username, headers, queryParams)
	if err != nil {
		return u, nil, err
	}
	defer resp.Body.Close()

	return u, resp, json.NewDecoder(resp.Body).Decode(&u)
}

// Create a new user
func (s *UsersService) CreateUser(body User, headers, queryParams map[string]interface{}) (*http.Response, error) {

	resp, err := s.client.doReqWithBody("POST", s.client.BaseURI+"/users", &body, headers, queryParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}
