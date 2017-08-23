package siteservice

import (
	"testing"

	"github.com/itsyouonline/identityserver/siteservice/website/packaged/html"
	"github.com/stretchr/testify/assert"
)

func TestLoginHtmlAvailable(t *testing.T) {

	htmlData, err := html.Asset(loginFileName)
	assert.NoError(t, err)
	assert.NotNil(t, htmlData)
}
