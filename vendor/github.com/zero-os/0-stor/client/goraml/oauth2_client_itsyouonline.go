package client

import (
	"fmt"
	"io/ioutil"
	"strings"
)

const (
	accessTokenURI = "https://itsyou.online/v1/oauth/access_token?response_type=id_token"
)

func (c *The_0_Stor) GetOauth2AccessToken(clientID, clientSecret string, scopes, audiences []string) (string, error) {
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
