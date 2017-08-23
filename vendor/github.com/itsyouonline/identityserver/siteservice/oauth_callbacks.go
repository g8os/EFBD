package siteservice

import (
	"encoding/json"
	"fmt"
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/itsyouonline/identityserver/db/user"
	"github.com/itsyouonline/identityserver/identityservice"
)

func (service *Service) FacebookCallback(w http.ResponseWriter, request *http.Request) {
	var code = request.URL.Query().Get("code")
	var redirectUri = "https://" + request.Host + "/facebook_callback"
	clientId, err := identityservice.GetOauthClientID("facebook")
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	clientSecret, err := identityservice.GetOauthSecret("facebook")
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	var oauthUrl = fmt.Sprintf("https://graph.facebook.com/v2.6/oauth/access_token?client_id=%s&client_secret=%s&code=%s&redirect_uri=%s",
		clientId, clientSecret, code, redirectUri)
	var fbInfo user.FBInfo
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", oauthUrl, nil)
	// Get access token from Github
	response, _ := httpClient.Do(req)
	facebookResponse := struct {
		Access_token string
		Token_type   string
		Expires_in   int
		Error        user.FacebookError
	}{}
	if err := json.NewDecoder(response.Body).Decode(&facebookResponse); err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if facebookResponse.Error.Message != "" {
		log.Error(facebookResponse.Error)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get Facebook user info using the Graph API.
	var fields = "id,picture,link,name"
	var apiUrl = fmt.Sprintf("https://graph.facebook.com/v2.6/me/?access_token=%s&fields=%s", facebookResponse.Access_token, fields)
	req, _ = http.NewRequest("GET", apiUrl, nil)
	response, _ = httpClient.Do(req)

	if err := json.NewDecoder(response.Body).Decode(&fbInfo); err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	// Save facebook info in database.
	userMgr := user.NewManager(request)
	var loggedInUser, e = service.GetLoggedInUser(request, w)
	if e != nil {
		log.Error(e)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	var fbAccount user.FacebookAccount
	fbAccount.Id = fbInfo.Id
	fbAccount.Name = fbInfo.Name
	fbAccount.Picture = fbInfo.Picture.Data.Url
	fbAccount.Link = fbInfo.Link
	if err := userMgr.UpdateFacebookAccount(loggedInUser, fbAccount); err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, request, "/", http.StatusTemporaryRedirect)

}

func (service *Service) GithubCallback(w http.ResponseWriter, request *http.Request) {
	var code = request.URL.Query().Get("code")
	// Get GitHub access token
	clientId, err := identityservice.GetOauthClientID("github")
	log.Info("clientId")
	log.Info(clientId)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	clientSecret, err := identityservice.GetOauthSecret("github")
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	redirect_uri := "https://" + request.Host + "/github_callback"
	var oauthUrl = fmt.Sprintf("https://github.com/login/oauth/access_token?&client_id=%s&client_secret=%s&code=%s&redirect_uri=%s",
		clientId, clientSecret, code, redirect_uri)
	var githubUserInfo user.GithubAccount
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", oauthUrl, nil)
	req.Header.Add("Accept", "application/json")
	// Get access token from Github
	response, err := httpClient.Do(req)
	if err != nil || response.StatusCode != 200 {
		log.Error(response.Status)
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	githubResponse := struct {
		Access_token      string
		Scope             string
		Token_type        string
		Error             string
		Error_description string
		Error_uri         string
	}{}
	if err := json.NewDecoder(response.Body).Decode(&githubResponse); err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if githubResponse.Error != "" {
		log.Error(githubResponse)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get user info from github
	var apiUrl = fmt.Sprintf("https://api.github.com/user?access_token=%s", githubResponse.Access_token)
	req, _ = http.NewRequest("GET", apiUrl, nil)
	// Get GitHub profile info from this user
	response, err = httpClient.Do(req)

	if err != nil || response.StatusCode != 200 {
		log.Error(response.Status)
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if err := json.NewDecoder(response.Body).Decode(&githubUserInfo); err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	userMgr := user.NewManager(request)
	// Save Github user info in db
	var loggedInUser, e = service.GetLoggedInUser(request, w)
	if e != nil {
		log.Error(e)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if err := userMgr.UpdateGithubAccount(loggedInUser, githubUserInfo); err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, request, "/", http.StatusTemporaryRedirect)

}
