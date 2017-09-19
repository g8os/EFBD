package server

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"golang.org/x/net/context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/zero-os/0-stor/grpc_store"
	"github.com/zero-os/0-stor/server/db/badger"
	"github.com/zero-os/0-stor/server/manager"
)

func getTestNamespaceAPI(t *testing.T) (*NamespaceAPI, func()) {
	tmpDir, err := ioutil.TempDir("", "0stortest")
	require.NoError(t, err)

	db, err := badger.New(path.Join(tmpDir, "data"), path.Join(tmpDir, "meta"))
	if err != nil {
		require.NoError(t, err)
	}

	clean := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	disableAuth()
	api := NewNamespaceAPI(db)
	return api, clean
}

func TestGetNamespace(t *testing.T) {
	api, clean := getTestNamespaceAPI(t)
	defer clean()

	label := "testnamespace"

	mgr := manager.NewNamespaceManager(api.db)
	err := mgr.Create(label)
	require.NoError(t, err)

	req := &pb.GetNamespaceRequest{Label: label}
	resp, err := api.Get(context.Background(), req)
	require.NoError(t, err)

	ns := resp.GetNamespace()
	assert.Equal(t, label, ns.GetLabel())
	assert.EqualValues(t, 0, ns.GetNrObjects())
	assert.EqualValues(t, 0, ns.GetReadRequestPerHour())
	assert.EqualValues(t, 0, ns.GetWriteRequestPerHour())
}
