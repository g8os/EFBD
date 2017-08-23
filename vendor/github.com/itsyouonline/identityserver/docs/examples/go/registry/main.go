package main

import (
	"fmt"
	"os"

	"github.com/itsyouonline/identityserver/clients/go/itsyouonline"
)

func parseArguments() (clientID, secret, host string) {
	arguments := os.Args
	if len(arguments) < 3 {
		fmt.Println("Usage: " + arguments[0] + " client_id client_secret [host]")
		os.Exit(1)
	}
	clientID = arguments[1]
	secret = arguments[2]
	if len(arguments) > 3 {
		host = arguments[3]
	} else {
		host = "https://itsyou.online"
	}
	return
}

func main() {
	clientID, secret, host := parseArguments()

	//Step 1: Create an itsyou.online client, log in with client credentials and create a registry entry
	authenticatedClient := itsyouonline.NewItsyouonline()
	authenticatedClient.BaseURI = host + "/api"
	username, globalid, _, err := authenticatedClient.LoginWithClientCredentials(clientID, secret)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	entry := itsyouonline.RegistryEntry{Key: "testscriptkey", Value: "testscriptvalue"}

	if username != "" {
		fmt.Printf("Creating/updating a registry entry for user: %s\n", username)
		authenticatedClient.Users.AddUserRegistryEntry(username, entry, nil, nil)
	}
	if globalid != "" {
		fmt.Printf("Creating/updating a registry entry for organization: %s\n", globalid)
		authenticatedClient.Organizations.AddOrganizationRegistryEntry(globalid, entry, nil, nil)
	}

	//Step 2: List the registry entries

	var entries []itsyouonline.RegistryEntry
	if username != "" {
		entries, _, _ = authenticatedClient.Users.ListUserRegistry(username, nil, nil)
	}
	if globalid != "" {
		entries, _, _ = authenticatedClient.Organizations.ListOrganizationRegistry(globalid, nil, nil)
	}

	for _, e := range entries {
		fmt.Printf("Key: %s - Value: %s\n", e.Key, e.Value)
	}

	//Step 3: Get the registry entry using an anonymous client
	anonymousClient := itsyouonline.NewItsyouonline()
	anonymousClient.BaseURI = host + "/api"

	var requestedEntry itsyouonline.RegistryEntry
	fmt.Println("Getting the registry entry using an unauthenticated client")
	if username != "" {
		requestedEntry, _, err = anonymousClient.Users.GetUserRegistryEntry("testscriptkey", username, nil, nil)
	}
	if globalid != "" {
		requestedEntry, _, err = anonymousClient.Organizations.GetOrganizationRegistryEntry("testscriptkey", username, nil, nil)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("Key: %s - Value: %s\n", requestedEntry.Key, requestedEntry.Value)

	//Step 4: Delete the entry from step 1
	if username != "" {
		fmt.Printf("Deleting a registry entry for user: %s\n", username)
		authenticatedClient.Users.DeleteUserRegistryEntry("testscriptkey", username, nil, nil)
	}
	if globalid != "" {
		fmt.Printf("Deleting a registry entry for organization: %s\n", globalid)
		authenticatedClient.Organizations.DeleteOrganizationRegistryEntry("testscriptkey", globalid, nil, nil)
	}
}
