package itsyouonline

import (
	"fmt"
	"io/ioutil"
	"strings"
)

const (
	accessTokenURI = "https://itsyou.online/v1/oauth/access_token"
)

func (c *Itsyouonline) GetOauth2AccessToken(clientID, clientSecret string, scopes, audiences []string) (string, error) {
	qp := map[string]interface{}{
		"grant_type":    "client_credentials",
		"client_id":     clientID,
		"client_secret": clientSecret,
	}

	if len(scopes) > 0 {
		qp["scope"] = strings.Join(scopes, ",")
	}

	if len(audiences) > 0 {
		qp["aud"] = strings.Join(audiences, ",")
	}

	resp, err := c.doReqNoBody("POST", accessTokenURI, nil, qp)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("failed to get access token, response code = %v", resp.StatusCode)
	}

	b, err := ioutil.ReadAll(resp.Body)
	return string(b), err
}
