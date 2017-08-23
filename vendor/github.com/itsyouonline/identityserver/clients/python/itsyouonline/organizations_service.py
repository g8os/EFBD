class OrganizationsService:
    def __init__(self, client):
        self.client = client



    def CreateNewOrganization(self, data, headers=None, query_params=None):
        """
        Create a new organization. 1 user should be in the owners list. Validation is performed to check if the securityScheme allows management on this user.
        It is method for POST /organizations
        """
        uri = self.client.base_url + "/organizations"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def GetOrganization(self, globalid, headers=None, query_params=None):
        """
        Get organization info
        It is method for GET /organizations/{globalid}
        """
        uri = self.client.base_url + "/organizations/"+globalid
        return self.client.session.get(uri, headers=headers, params=query_params)


    def CreateNewSubOrganization(self, data, globalid, headers=None, query_params=None):
        """
        Create a new suborganization.
        It is method for POST /organizations/{globalid}
        """
        uri = self.client.base_url + "/organizations/"+globalid
        return self.client.post(uri, data, headers=headers, params=query_params)


    def UpdateOrganization(self, data, globalid, headers=None, query_params=None):
        """
        Update organization info
        It is method for PUT /organizations/{globalid}
        """
        uri = self.client.base_url + "/organizations/"+globalid
        return self.client.put(uri, data, headers=headers, params=query_params)


    def DeleteOrganization(self, globalid, headers=None, query_params=None):
        """
        Deletes an organization and all data linked to it (join-organization-invitations, oauth_access_tokens, oauth_clients, logo)
        It is method for DELETE /organizations/{globalid}
        """
        uri = self.client.base_url + "/organizations/"+globalid
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def Set2faValidityTime(self, data, globalid, headers=None, query_params=None):
        """
        Update the 2FA validity time for the organization
        It is method for POST /organizations/{globalid}/2fa/validity
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/2fa/validity"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def Get2faValidityTime(self, globalid, headers=None, query_params=None):
        """
        Get the 2FA validity time for the organization, in seconds
        It is method for GET /organizations/{globalid}/2fa/validity
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/2fa/validity"
        return self.client.session.get(uri, headers=headers, params=query_params)


    def GetOrganizationAPIKeyLabels(self, globalid, headers=None, query_params=None):
        """
        Get the list of active api keys.
        It is method for GET /organizations/{globalid}/apikeys
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/apikeys"
        return self.client.session.get(uri, headers=headers, params=query_params)


    def CreateNewOrganizationAPIKey(self, data, globalid, headers=None, query_params=None):
        """
        Create a new API Key, a secret itself should not be provided, it will be generated serverside.
        It is method for POST /organizations/{globalid}/apikeys
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/apikeys"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def UpdateOrganizationAPIKey(self, data, label, globalid, headers=None, query_params=None):
        """
        Updates the label or other properties of a key.
        It is method for PUT /organizations/{globalid}/apikeys/{label}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/apikeys/"+label
        return self.client.put(uri, data, headers=headers, params=query_params)


    def GetOrganizationAPIKey(self, label, globalid, headers=None, query_params=None):
        """
        Get an api key from an organization
        It is method for GET /organizations/{globalid}/apikeys/{label}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/apikeys/"+label
        return self.client.session.get(uri, headers=headers, params=query_params)


    def DeleteOrganizationAPIKey(self, label, globalid, headers=None, query_params=None):
        """
        Removes an API key
        It is method for DELETE /organizations/{globalid}/apikeys/{label}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/apikeys/"+label
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def GetOrganizationContracts(self, globalid, headers=None, query_params=None):
        """
        Get the contracts where the organization is 1 of the parties. Order descending by date.
        It is method for GET /organizations/{globalid}/contracts
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/contracts"
        return self.client.session.get(uri, headers=headers, params=query_params)


    def CreateOrganizationContracty(self, data, globalid, headers=None, query_params=None):
        """
        Create a new contract.
        It is method for POST /organizations/{globalid}/contracts
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/contracts"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def SetDescription(self, data, globalid, headers=None, query_params=None):
        """
        Set the description for this organization for a given language key
        It is method for POST /organizations/{globalid}/description
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/description"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def UpdateDescription(self, data, globalid, headers=None, query_params=None):
        """
        Update the description for this organization for a given language key
        It is method for PUT /organizations/{globalid}/description
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/description"
        return self.client.put(uri, data, headers=headers, params=query_params)


    def GetDescription(self, langkey, globalid, headers=None, query_params=None):
        """
        Get the description for an organization for this langkey
        It is method for GET /organizations/{globalid}/description/{langkey}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/description/"+langkey
        return self.client.session.get(uri, headers=headers, params=query_params)


    def DeleteDescription(self, langkey, globalid, headers=None, query_params=None):
        """
        Delete the description for this organization for a given language key
        It is method for DELETE /organizations/{globalid}/description/{langkey}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/description/"+langkey
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def GetDescriptionWithFallback(self, langkey, globalid, headers=None, query_params=None):
        """
        Get the description for an organization for this langkey, try to use the English is there is no description for this langkey
        It is method for GET /organizations/{globalid}/description/{langkey}/withfallback
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/description/"+langkey+"/withfallback"
        return self.client.session.get(uri, headers=headers, params=query_params)


    def CreateOrganizationDns(self, data, globalid, headers=None, query_params=None):
        """
        Creates a new DNS name associated with an organization
        It is method for POST /organizations/{globalid}/dns
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/dns"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def UpdateOrganizationDns(self, data, dnsname, globalid, headers=None, query_params=None):
        """
        Updates an existing DNS name associated with an organization
        It is method for PUT /organizations/{globalid}/dns/{dnsname}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/dns/"+dnsname
        return self.client.put(uri, data, headers=headers, params=query_params)


    def DeleteOrganizationDns(self, dnsname, globalid, headers=None, query_params=None):
        """
        Removes a DNS name associated with an organization
        It is method for DELETE /organizations/{globalid}/dns/{dnsname}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/dns/"+dnsname
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def GetInvitations(self, globalid, headers=None, query_params=None):
        """
        Get the list of pending invitations for users to join this organization.
        It is method for GET /organizations/{globalid}/invitations
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/invitations"
        return self.client.session.get(uri, headers=headers, params=query_params)


    def RemovePendingOrganizationInvitation(self, username, globalid, headers=None, query_params=None):
        """
        Cancel a pending invitation.
        It is method for DELETE /organizations/{globalid}/invitations/{username}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/invitations/"+username
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def UserIsMember(self, username, globalid, headers=None, query_params=None):
        """
        Checks if the user has memberschip rights on the organization
        It is method for GET /organizations/{globalid}/ismember/{username}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/ismember/"+username
        return self.client.session.get(uri, headers=headers, params=query_params)


    def DeleteOrganizationLogo(self, globalid, headers=None, query_params=None):
        """
        Removes the Logo from an organization
        It is method for DELETE /organizations/{globalid}/logo
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/logo"
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def SetOrganizationLogo(self, data, globalid, headers=None, query_params=None):
        """
        Set the organization Logo for the organization
        It is method for PUT /organizations/{globalid}/logo
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/logo"
        return self.client.put(uri, data, headers=headers, params=query_params)


    def GetOrganizationLogo(self, globalid, headers=None, query_params=None):
        """
        Get the Logo from an organization
        It is method for GET /organizations/{globalid}/logo
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/logo"
        return self.client.session.get(uri, headers=headers, params=query_params)


    def UpdateOrganizationMemberShip(self, data, globalid, headers=None, query_params=None):
        """
        Update an organization membership
        It is method for PUT /organizations/{globalid}/members
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/members"
        return self.client.put(uri, data, headers=headers, params=query_params)


    def AddOrganizationMember(self, data, globalid, headers=None, query_params=None):
        """
        Invite someone to become member of an organization.
        It is method for POST /organizations/{globalid}/members
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/members"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def RemoveOrganizationMember(self, username, globalid, headers=None, query_params=None):
        """
        Remove a member from an organization.
        It is method for DELETE /organizations/{globalid}/members/{username}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/members/"+username
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def RejectOrganizationInvite(self, invitingorg, role, globalid, headers=None, query_params=None):
        """
        Reject the invite for one of your organizations
        It is method for DELETE /organizations/{globalid}/organizations/{invitingorg}/roles/{role}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/organizations/"+invitingorg+"/roles/"+role
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def AcceptOrganizationInvite(self, data, invitingorg, role, globalid, headers=None, query_params=None):
        """
        Accept the invite for one of your organizations
        It is method for POST /organizations/{globalid}/organizations/{invitingorg}/roles/{role}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/organizations/"+invitingorg+"/roles/"+role
        return self.client.post(uri, data, headers=headers, params=query_params)


    def UpdateOrganizationOrgMemberShip(self, data, globalid, headers=None, query_params=None):
        """
        Update the membership status of an organization
        It is method for PUT /organizations/{globalid}/orgmembers
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/orgmembers"
        return self.client.put(uri, data, headers=headers, params=query_params)


    def SetOrgMember(self, data, globalid, headers=None, query_params=None):
        """
        Add another organization as a member of this one
        It is method for POST /organizations/{globalid}/orgmembers
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/orgmembers"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def AddIncludeSubOrgsOf(self, data, globalid, headers=None, query_params=None):
        """
        Add an orgmember or orgowner organization to the includesuborgsof list
        It is method for POST /organizations/{globalid}/orgmembers/includesuborgs
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/orgmembers/includesuborgs"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def RemoveIncludeSubOrgsOf(self, orgmember, globalid, headers=None, query_params=None):
        """
        Remove an orgmember or orgowner organization to the includesuborgsof list
        It is method for DELETE /organizations/{globalid}/orgmembers/includesuborgs/{orgmember}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/orgmembers/includesuborgs/"+orgmember
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def DeleteOrgMember(self, globalid2, globalid, headers=None, query_params=None):
        """
        Remove an organization as a member
        It is method for DELETE /organizations/{globalid}/orgmembers/{globalid2}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/orgmembers/"+globalid2
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def SetOrgOwner(self, data, globalid, headers=None, query_params=None):
        """
        Add another organization as an owner of this one
        It is method for POST /organizations/{globalid}/orgowners
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/orgowners"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def DeleteOrgOwner(self, globalid2, globalid, headers=None, query_params=None):
        """
        Remove an organization as an owner
        It is method for DELETE /organizations/{globalid}/orgowners/{globalid2}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/orgowners/"+globalid2
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def AddOrganizationOwner(self, data, globalid, headers=None, query_params=None):
        """
        Invite someone to become owner of an organization.
        It is method for POST /organizations/{globalid}/owners
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/owners"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def RemoveOrganizationOwner(self, username, globalid, headers=None, query_params=None):
        """
        Remove an owner from organization
        It is method for DELETE /organizations/{globalid}/owners/{username}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/owners/"+username
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def AddOrganizationRegistryEntry(self, data, globalid, headers=None, query_params=None):
        """
        Adds a RegistryEntry to the organization's registry, if the key is already used, it is overwritten.
        It is method for POST /organizations/{globalid}/registry
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/registry"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def ListOrganizationRegistry(self, globalid, headers=None, query_params=None):
        """
        Lists the RegistryEntries in an organization's registry.
        It is method for GET /organizations/{globalid}/registry
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/registry"
        return self.client.session.get(uri, headers=headers, params=query_params)


    def GetOrganizationRegistryEntry(self, key, globalid, headers=None, query_params=None):
        """
        Get a RegistryEntry from the organization's registry.
        It is method for GET /organizations/{globalid}/registry/{key}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/registry/"+key
        return self.client.session.get(uri, headers=headers, params=query_params)


    def DeleteOrganizationRegistryEntry(self, key, globalid, headers=None, query_params=None):
        """
        Removes a RegistryEntry from the organization's registry
        It is method for DELETE /organizations/{globalid}/registry/{key}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/registry/"+key
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def AddRequiredScope(self, data, globalid, headers=None, query_params=None):
        """
        Adds a required scope
        It is method for POST /organizations/{globalid}/requiredscopes
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/requiredscopes"
        return self.client.post(uri, data, headers=headers, params=query_params)


    def DeleteRequiredScope(self, requiredscope, globalid, headers=None, query_params=None):
        """
        Deletes a required scope
        It is method for DELETE /organizations/{globalid}/requiredscopes/{requiredscope}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/requiredscopes/"+requiredscope
        return self.client.session.delete(uri, headers=headers, params=query_params)


    def UpdateRequiredScope(self, data, requiredscope, globalid, headers=None, query_params=None):
        """
        Updates a required scope
        It is method for PUT /organizations/{globalid}/requiredscopes/{requiredscope}
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/requiredscopes/"+requiredscope
        return self.client.put(uri, data, headers=headers, params=query_params)


    def GetOrganizationTree(self, globalid, headers=None, query_params=None):
        """
        Tree structure of all suborganizations
        It is method for GET /organizations/{globalid}/tree
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/tree"
        return self.client.session.get(uri, headers=headers, params=query_params)


    def GetOrganizationUsers(self, globalid, headers=None, query_params=None):
        """
        Get all users from this organization, not including suborganizations.
        It is method for GET /organizations/{globalid}/users
        """
        uri = self.client.base_url + "/organizations/"+globalid+"/users"
        return self.client.session.get(uri, headers=headers, params=query_params)
