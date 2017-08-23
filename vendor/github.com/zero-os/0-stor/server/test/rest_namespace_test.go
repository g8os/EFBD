package test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/server/api/rest"
)

func TestGetNameSpace(t *testing.T) {
	url, db, iyoOrganization, iyoClient, permissions, clean := getTestRestAPI(t)
	defer clean()

	err := iyoClient.CreateNamespace("namespace2")
	require.NoError(t, err)

	token, err := iyoClient.CreateJWT("namespace2", permissions["read"])
	require.NoError(t, err)

	exists, err := db.Exists([]byte(iyoOrganization + "_0stor_namespace2"))
	require.NoError(t, err)
	assert.False(t, exists)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", url+"/namespaces/"+iyoOrganization+"_0stor_namespace2", nil)
	req.Header.Set("Authorization", token)

	resp, _ := client.Do(req)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	exists, err = db.Exists([]byte(iyoOrganization + "_0stor_namespace2"))
	require.NoError(t, err)
	assert.True(t, exists)

	result := rest.Namespace{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err, "error decoding response")

	assert.Equal(t, result.Label, iyoOrganization+"_0stor_namespace2")
	assert.Equal(t, result.Stats.NrObjects, int64(0))
}
