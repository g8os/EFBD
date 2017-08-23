package siteservice

import (
	"testing"

	"github.com/itsyouonline/identityserver/siteservice/website/packaged/html"
	"github.com/stretchr/testify/assert"
)

func TestRegistrationHtmlAvailable(t *testing.T) {

	htmlData, err := html.Asset(registrationFileName)
	assert.NoError(t, err)
	assert.NotNil(t, htmlData)
}
