package siteservice

import (
	"testing"

	"github.com/itsyouonline/identityserver/siteservice/website/packaged/html"
	"github.com/stretchr/testify/assert"
)

func TestServiceHtmlAvailable(t *testing.T) {

	htmlData, err := html.Asset(homepageFileName)
	assert.NoError(t, err)
	assert.NotNil(t, htmlData)

	htmlData, err = html.Asset(mainpageFileName)
	assert.NoError(t, err)
	assert.NotNil(t, htmlData)

}
