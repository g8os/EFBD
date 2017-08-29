package main

import (
	"fmt"
	"os"

	"github.com/itsyouonline/identityserver/clients/go/itsyouonline"
)

func parseArguments() (clientID, secret, parentorgGlobalID string, suborgGlobalID string, host string) {
	arguments := os.Args
	if len(arguments) < 5 {
		fmt.Println("Usage: " + arguments[0] + " client_id client_secret parentorg_globalid suborg_globalid [host]")
		os.Exit(1)
	}
	argumentIndex := 1
	clientID = arguments[argumentIndex]
	argumentIndex++
	secret = arguments[argumentIndex]
	argumentIndex++
	parentorgGlobalID = arguments[argumentIndex]
	argumentIndex++
	suborgGlobalID = arguments[argumentIndex]
	argumentIndex++

	if len(arguments) > argumentIndex {
		host = arguments[argumentIndex]
	} else {
		host = "https://itsyou.online"
	}
	return
}

func main() {
	clientID, secret, parentorgGlobalID, suborgGlobalID, host := parseArguments()

	//Step 0: Authenticate
	authenticatedClient := itsyouonline.NewItsyouonline()
	authenticatedClient.BaseURI = host + "/api"
	authenticatedUsername, authenticatedGlobalID, _, err := authenticatedClient.LoginWithClientCredentials(clientID, secret)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if authenticatedUsername != "" {
		fmt.Printf("Authenticated with user %s\n", authenticatedUsername)
	}
	if authenticatedGlobalID != "" {
		fmt.Printf("Authenticated with organization %s\n", authenticatedGlobalID)
	}

	//Step 1: Create the suborganization
	suborg := itsyouonline.Organization{Globalid: suborgGlobalID}

	fmt.Printf("Creating suborganization %s in parent %s\n", suborgGlobalID, parentorgGlobalID)
	_, _, err = authenticatedClient.Organizations.CreateNewSubOrganization(parentorgGlobalID, suborg, nil, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	//Step2: Access the suborganization
	fmt.Printf("Getting the suborganization information\n")
	suborgView, _, err := authenticatedClient.Organizations.GetOrganization(suborgGlobalID, nil, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(suborgView)

	//Step 3: Delete the entry from step 1
	fmt.Printf("Deleting organization %s\n", suborgGlobalID)
	_, err = authenticatedClient.Organizations.DeleteOrganization(suborgGlobalID, nil, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
