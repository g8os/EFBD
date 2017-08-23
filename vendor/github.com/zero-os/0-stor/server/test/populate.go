package test

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/manager"
)

// PopulateDB insert some test object in the db for testing purpose
func PopulateDB(t *testing.T, db db.DB) (string, [][]byte) {
	label := "testnamespace"

	nsMgr := manager.NewNamespaceManager(db)
	objMgr := manager.NewObjectManager(label, db)
	err := nsMgr.Create(label)
	require.NoError(t, err)

	bufList := make([][]byte, 10)

	for i := 0; i < 10; i++ {
		bufList[i] = make([]byte, 1024*1024)
		_, err = rand.Read(bufList[i])
		require.NoError(t, err)

		refList := []string{"user1", "user2"}
		key := fmt.Sprintf("testkey%d", i)

		err = objMgr.Set([]byte(key), bufList[i], refList)
		require.NoError(t, err)
	}

	return label, bufList
}
