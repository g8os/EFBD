package test

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"path"
	"testing"

	log "github.com/Sirupsen/logrus"
	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/server/api/rest"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/db/badger"
	"github.com/zero-os/0-stor/server/jwt"
	"github.com/zero-os/0-stor/server/manager"
	"github.com/zero-os/0-stor/server/storserver"
	"github.com/zero-os/0-stor/stubs"
)

func TestMain(m *testing.M) {
	log.SetLevel(log.DebugLevel)
	os.Exit(m.Run())
}

func getTestRestAPI(t *testing.T) (string, db.DB, string, stubs.IYOClient, map[string]itsyouonline.Permission, func()) {

	tmpDir, err := ioutil.TempDir("", "0stortest")
	require.NoError(t, err)

	db, err := badger.New(path.Join(tmpDir, "data"), path.Join(tmpDir, "meta"))
	if err != nil {
		require.NoError(t, err)
	}

	r := mux.NewRouter()
	api := rest.NewNamespaceAPI(db)
	rest.NamespacesInterfaceRoutes(r, api, db)
	srv := httptest.NewServer(r)

	clean := func() {
		os.RemoveAll(tmpDir)
		srv.Close()
	}

	iyoCl, organization := getIYOClient(t)

	permissions := make(map[string]itsyouonline.Permission)

	permissions["read"] = itsyouonline.Permission{Read: true}
	permissions["all"] = itsyouonline.Permission{Read: true, Write: true, Delete: true}
	permissions["write"] = itsyouonline.Permission{Write: true}
	permissions["delete"] = itsyouonline.Permission{Delete: true}

	return srv.URL, db, organization, iyoCl, permissions, clean
}

func getTestGRPCServer(t *testing.T) (storserver.StoreServer, stubs.IYOClient, string, func()) {
	tmpDir, err := ioutil.TempDir("", "0stortest")
	require.NoError(t, err)

	server, err := storserver.NewGRPC(path.Join(tmpDir, "data"), path.Join(tmpDir, "meta"))
	require.NoError(t, err)

	_, err = server.Listen("localhost:0")
	require.NoError(t, err, "server failed to start listening")

	jwtCreater, organization := getIYOClient(t)

	clean := func() {
		fmt.Sprintln("clean called")
		server.Close()
		os.RemoveAll(tmpDir)
	}

	return server, jwtCreater, organization, clean
}

func getIYOClient(t testing.TB) (stubs.IYOClient, string) {
	pubKey, err := ioutil.ReadFile("../../devcert/jwt_pub.pem")
	require.NoError(t, err)
	jwt.SetJWTPublicKey(string(pubKey))

	b, err := ioutil.ReadFile("../../devcert/jwt_key.pem")
	require.NoError(t, err)

	key, err := jwtgo.ParseECPrivateKeyFromPEM(b)
	require.NoError(t, err)

	organization := "testorg"
	jwtCreater, err := stubs.NewStubIYOClient(organization, key)
	require.NoError(t, err, "failt to create MockJWTCreater")

	return jwtCreater, organization
}

func populateDB(t *testing.T, namespace string, db db.DB) [][]byte {
	nsMgr := manager.NewNamespaceManager(db)
	objMgr := manager.NewObjectManager(namespace, db)
	err := nsMgr.Create(namespace)
	require.NoError(t, err)

	bufList := make([][]byte, 10)

	for i := 0; i < 10; i++ {
		bufList[i] = make([]byte, 1024*1024)
		_, err = rand.Read(bufList[i])
		require.NoError(t, err)

		refList := []string{
			"user1", "user2",
		}
		key := fmt.Sprintf("testkey%d", i)

		err = objMgr.Set([]byte(key), bufList[i], refList)
		require.NoError(t, err)
	}

	return bufList
}
