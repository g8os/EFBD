package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/server/api/rest"
	"github.com/zero-os/0-stor/server/db"
	//"fmt"
	//"sort"
	"fmt"
	"sort"
)

func TestCreateObject(t *testing.T) {
	url, database, iyoOrganization, iyoClient, permissions, clean := getTestRestAPI(t)
	defer clean()

	// Trying to create invalid object. (size > 1mB)
	//@TODO: FIXME this gives an error in Httpserver
	//2017/07/17 17:29:37 httptest.Server blocked in Close after 5 seconds, waiting for connections:
	//*net.TCPConn 0xc424fac0b8 127.0.0.1:34030 in state active

	//body = &bytes.Buffer{}
	//data := make([]byte, 1025*1024)
	//obj := models.Object{
	//	Data: string(data[:]),
	//	Id:   "myobject",
	//}
	//err = json.NewEncoder(body).Encode(obj)
	//require.NoError(t, err)
	//resp, err = http.Post(url+"/namespaces/mynamespace/objects", "application/json", body)
	//require.NoError(t, err)

	// Trying to create valid object.
	body := &bytes.Buffer{}
	obj := rest.Object{
		Data: "********************************abcdef",
		Id:   "myobject",
		ReferenceList: []rest.ReferenceID{
			"ref1",
		},
	}
	err := json.NewEncoder(body).Encode(obj)
	require.NoError(t, err)

	err = iyoClient.CreateNamespace("namespace1")
	require.NoError(t, err)

	token, err := iyoClient.CreateJWT("namespace1", permissions["write"])
	require.NoError(t, err)

	nsid := iyoOrganization + "_0stor_namespace1"
	client := &http.Client{}
	req, _ := http.NewRequest("POST", url+"/namespaces/"+nsid+"/objects", body)
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)

	require.NoError(t, err)

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	exists, err := database.Exists([]byte(nsid + ":myobject"))
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestGetObject(t *testing.T) {
	url, database, iyoOrganization, iyoClient, permissions, clean := getTestRestAPI(t)

	defer clean()

	// create an object.

	obj := db.Object{
		Data: []byte("********************************abcdef"),
	}

	b, err := obj.Encode()
	require.NoError(t, err)

	nsid := iyoOrganization + "_0stor_namespace1"
	err = database.Set([]byte(nsid+":myobject"), b)

	err = iyoClient.CreateNamespace("namespace1")
	require.NoError(t, err)

	token, err := iyoClient.CreateJWT("namespace1", permissions["read"])
	require.NoError(t, err)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url+"/namespaces/"+nsid+"/objects/myobject", nil)
	require.NoError(t, err)

	req.Header.Set("Authorization", token)

	resp, err := client.Do(req)

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	result := rest.Object{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)

	require.Equal(t, result.Id, "myobject")
	require.Equal(t, result.Data, string(obj.Data))

	req, err = http.NewRequest("GET", url+"/namespaces/"+nsid+"/objects/object2", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", token)
	resp, err = client.Do(req)

	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestListObjects(t *testing.T) {
	url, database, iyoOrganization, iyoClient, permissions, clean := getTestRestAPI(t)
	defer clean()

	nsid := iyoOrganization + "_0stor_namespace1"

	objects := []string{"obj1", "obj2", "obj3"}
	for _, label := range objects {

		key := fmt.Sprintf("%s:%s", nsid, label)

		obj := db.Object{
			Data: []byte("********************************abcdef"),
		}

		b, err := obj.Encode()
		require.NoError(t, err)

		err = database.Set([]byte(key), b)
	}

	err := iyoClient.CreateNamespace("namespace1")
	require.NoError(t, err)

	token, err := iyoClient.CreateJWT("namespace1", permissions["read"])
	require.NoError(t, err)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", url+"/namespaces/"+nsid+"/objects", nil)
	req.Header.Set("Authorization", token)

	resp, err := client.Do(req)

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	result := []string{}
	err = json.NewDecoder(resp.Body).Decode(&result)

	require.Equal(t, len(result), 3)
	sort.Strings(result)
	require.Equal(t, result, objects)
}

func TestDeleteObject(t *testing.T) {
	url, database, iyoOrganization, iyoClient, permissions, clean := getTestRestAPI(t)
	defer clean()

	nsid := iyoOrganization + "_0stor_namespace1"

	// create an object.

	obj := db.Object{
		Data: []byte("********************************abcdef"),
	}

	b, err := obj.Encode()
	require.NoError(t, err)

	err = database.Set([]byte(nsid+":myobject"), b)

	err = iyoClient.CreateNamespace("namespace1")
	require.NoError(t, err)

	token, err := iyoClient.CreateJWT("namespace1", permissions["all"])
	require.NoError(t, err)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url+"/namespaces/"+nsid+"/objects/myobject", nil)
	require.NoError(t, err)

	req.Header.Set("Authorization", token)

	resp, err := client.Do(req)

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequest("DELETE", url+"/namespaces/"+nsid+"/objects/myobject", &bytes.Buffer{})
	req.Header.Add("Content-Type", "application/json")
	req.Header.Set("Authorization", token)

	require.NoError(t, err)
	cli := http.DefaultClient

	resp, err = cli.Do(req)

	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// No actual delete yet

	//exists, err := database.Exists([]byte("mynamespace:myobject"))
	//require.NoError(t, err)
	//require.False(t, exists)
}

func TestCheckObjects(t *testing.T) {
	url, database, iyoOrganization, iyoClient, permissions, clean := getTestRestAPI(t)
	defer clean()

	nsid := iyoOrganization + "_0stor_namespace1"

	objects := []string{"obj1", "obj2", "obj3"}
	for _, label := range objects {

		key := fmt.Sprintf("%s:%s", nsid, label)

		obj := db.Object{
			Data: []byte("********************************abcdef"),
		}

		b, err := obj.Encode()
		require.NoError(t, err)

		err = database.Set([]byte(key), b)
	}

	err := iyoClient.CreateNamespace("namespace1")
	require.NoError(t, err)

	token, err := iyoClient.CreateJWT("namespace1", permissions["read"])
	require.NoError(t, err)

	client := &http.Client{}
	body := struct {
		Ids []string `json:"ids"`
	}{
		Ids: []string{"obj1", "obj2", "obj3"},
	}
	buf := &bytes.Buffer{}
	err = json.NewEncoder(buf).Encode(body)
	require.NoError(t, err, "failt to encode request body")

	req, _ := http.NewRequest("GET", url+"/namespaces/"+nsid+"/check", buf)
	req.Header.Set("Authorization", token)

	resp, err := client.Do(req)

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	result := []rest.CheckStatus{}
	err = json.NewDecoder(resp.Body).Decode(&result)

	require.Equal(t, len(result), 3)
	for _, res := range result {
		assert.Equal(t, rest.EnumCheckStatusStatusok, res.Status)
	}
}
