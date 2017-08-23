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
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/server/api/rest"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/db/badger"
	"github.com/zero-os/0-stor/server/manager"
	"github.com/zero-os/0-stor/server/storserver"
)

func TestMain(m *testing.M) {
	log.SetLevel(log.DebugLevel)
	os.Exit(m.Run())
}

func getTestRestAPI(t *testing.T) (string, db.DB, string, *itsyouonline.Client, map[string]itsyouonline.Permission, func()) {

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

	iyoClientID := os.Getenv("iyo_client_id")
	iyoSecret := os.Getenv("iyo_secret")
	iyoOrganization := os.Getenv("iyo_organization")

	if iyoClientID == "" {
		log.Fatal("[iyo] Missing (iyo_client_id) environement variable")

	}

	if iyoSecret == "" {
		log.Fatal("[iyo] Missing (iyo_secret) environement variable")

	}

	if iyoOrganization == "" {
		log.Fatal("[iyo] Missing (iyo_organization) environement variable")

	}

	iyoClient, iyoOrganization := getIYOClient(t)

	permissions := make(map[string]itsyouonline.Permission)

	permissions["read"] = itsyouonline.Permission{Read: true}
	permissions["all"] = itsyouonline.Permission{Read: true, Write: true, Delete: true}
	permissions["write"] = itsyouonline.Permission{Write: true}
	permissions["delete"] = itsyouonline.Permission{Delete: true}

	return srv.URL, db, iyoOrganization, iyoClient, permissions, clean
}

func getTestGRPCServer(t *testing.T) (storserver.StoreServer, *itsyouonline.Client, string, func()) {
	tmpDir, err := ioutil.TempDir("", "0stortest")
	require.NoError(t, err)

	server, err := storserver.NewGRPC(path.Join(tmpDir, "data"), path.Join(tmpDir, "meta"))
	require.NoError(t, err)

	_, err = server.Listen("localhost:0")
	require.NoError(t, err, "server failed to start listening")

	iyoClient, iyoOrg := getIYOClient(t)

	iyoClient.CreateJWT("testnamespace", itsyouonline.Permission{})

	clean := func() {
		fmt.Sprintln("clean called")
		server.Close()
		os.RemoveAll(tmpDir)
	}

	return server, iyoClient, iyoOrg, clean
}

func getIYOClient(t *testing.T) (*itsyouonline.Client, string) {
	iyoClientID := os.Getenv("iyo_client_id")
	iyoSecret := os.Getenv("iyo_secret")
	iyoOrganization := os.Getenv("iyo_organization")

	if iyoClientID == "" {
		log.Fatal("[iyo] Missing (iyo_client_id) environement variable")

	}

	if iyoSecret == "" {
		log.Fatal("[iyo] Missing (iyo_secret) environement variable")

	}

	if iyoOrganization == "" {
		log.Fatal("[iyo] Missing (iyo_organization) environement variable")

	}

	iyoClient := itsyouonline.NewClient(iyoOrganization, iyoClientID, iyoSecret)

	return iyoClient, iyoOrganization
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
