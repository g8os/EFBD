package siteservice

import (
	"net/http"
	"net/url"
)

//ShowAuthorizeForm shows the scopes an application requested and asks a user for confirmation
func (service *Service) ShowAuthorizeForm(w http.ResponseWriter, r *http.Request) {
	redirectURI := "#" + r.RequestURI
	parameters := make(url.Values)
	redirectURI += parameters.Encode()
	http.Redirect(w, r, redirectURI, http.StatusFound)
}
