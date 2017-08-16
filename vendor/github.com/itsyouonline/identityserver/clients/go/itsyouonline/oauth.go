package itsyouonline

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// LoginWithClientCredentials login to itsyouonline using the client ID and client secret.
// If succeed:
//  - the authenticated user in the username or the globalid of the authenticated organization
//  - returns the oauth2 access token
//  - set AuthHeader to `token TOKEN_VALUE`.
func (c *Itsyouonline) LoginWithClientCredentials(clientID, clientSecret string) (username, globalid, token string, err error) {
	// build request
	url := strings.TrimSuffix(c.BaseURI, "/api")
	req, err := http.NewRequest("POST", url+"/v1/oauth/access_token", nil)
	if err != nil {
		return
	}

	// request query params
	qs := map[string]interface{}{
		"grant_type":    "client_credentials",
		"client_id":     clientID,
		"client_secret": clientSecret,
	}
	q := req.URL.Query()
	for k, v := range qs {
		q.Add(k, fmt.Sprintf("%v", v))
	}
	req.URL.RawQuery = q.Encode()

	// do the request
	rsp, err := c.client.Do(req)
	if err != nil {
		return
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != 200 {
		err = fmt.Errorf("invalid response's status code :%v", rsp.StatusCode)
		return
	}

	// decode
	var jsonResp map[string]interface{}
	if err = json.NewDecoder(rsp.Body).Decode(&jsonResp); err != nil {
		return
	}
	val, ok := jsonResp["access_token"]
	if !ok {
		err = fmt.Errorf("no token found")
		return
	}
	token = fmt.Sprintf("%v", val)

	if val, ok = jsonResp["info"]; ok {
		if info, ok := val.(map[string]interface{}); ok {
			usernameInterface := info["username"]
			if usernameInterface != nil {
				username = usernameInterface.(string)
			}
			globalidInterface := info["globalid"]
			if globalidInterface != nil {
				globalid = globalidInterface.(string)
			}
		}
	}

	c.AuthHeader = "token " + token

	return

}

// CreateJWTToken creates JWT token with scope=scopes
// and audience=auds.
// To execute it, client need to be logged in.
func (c *Itsyouonline) CreateJWTToken(scopes, auds []string) (string, error) {
	// build request
	url := strings.TrimSuffix(c.BaseURI, "/api")
	req, err := http.NewRequest("GET", url+"/v1/oauth/jwt", nil)
	if err != nil {
		return "", err
	}

	// set auth header
	if c.AuthHeader == "" {
		return "", fmt.Errorf("you need to create an oauth token in order to create JWT token")
	}

	req.Header.Set("Authorization", c.AuthHeader)

	// query params
	q := req.URL.Query()
	if len(scopes) > 0 {
		q.Add("scope", strings.Join(scopes, ","))
	}
	if len(auds) > 0 {
		q.Add("aud", strings.Join(auds, ","))
	}
	req.URL.RawQuery = q.Encode()

	// do the request
	rsp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != 200 {
		return "", fmt.Errorf("invalid response's status code :%v", rsp.StatusCode)
	}

	b, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
