package server

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"golang.org/x/net/context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/zero-os/0-stor/grpc_store"
	"github.com/zero-os/0-stor/server/db"
	"github.com/zero-os/0-stor/server/db/badger"
	"github.com/zero-os/0-stor/server/manager"
)

func getTestObjectAPI(t *testing.T) (*ObjectAPI, func()) {
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
	return NewObjectAPI(db), clean
}

func populateDB(t *testing.T, db db.DB) (string, [][]byte) {
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

		refList := []string{
			"user1", "user2",
		}
		key := fmt.Sprintf("testkey%d", i)

		err = objMgr.Set([]byte(key), bufList[i], refList)
		require.NoError(t, err)
	}

	return label, bufList
}

func TestCreateObject(t *testing.T) {
	api, clean := getTestObjectAPI(t)
	defer clean()

	label := "testnamespace"

	nsMgr := manager.NewNamespaceManager(api.db)
	objMgr := manager.NewObjectManager(label, api.db)
	err := nsMgr.Create(label)
	require.NoError(t, err)

	buf := make([]byte, 1024*1024)
	_, err = rand.Read(buf)
	require.NoError(t, err)

	req := &pb.CreateObjectRequest{
		Label: label,
		Object: &pb.Object{
			Key:           []byte("testkey"),
			Value:         buf,
			ReferenceList: []string{"user1", "user2"},
		},
	}

	_, err = api.Create(context.Background(), req)
	require.NoError(t, err)

	obj, err := objMgr.Get([]byte("testkey"))
	require.NoError(t, err)
	assert.Equal(t, buf, obj.Data)
	assert.EqualValues(t, []byte("user1"), bytes.Trim(obj.ReferenceList[0][:], "\x00"))
	assert.EqualValues(t, []byte("user2"), bytes.Trim(obj.ReferenceList[1][:], "\x00"))
	assert.EqualValues(t, []byte(nil), bytes.Trim(obj.ReferenceList[2][:], "\x00"))
}

func TestGetObject(t *testing.T) {
	api, clean := getTestObjectAPI(t)
	defer clean()

	label, bufList := populateDB(t, api.db)

	t.Run("valid", func(t *testing.T) {
		key := []byte("testkey0")
		req := &pb.GetObjectRequest{
			Label: label,
			Key:   key,
		}

		resp, err := api.Get(context.Background(), req)
		require.NoError(t, err)

		obj := resp.GetObject()

		assert.Equal(t, key, obj.GetKey())
		assert.Equal(t, bufList[0], obj.GetValue())
		assert.Equal(t, []string{"user1", "user2"}, obj.GetReferenceList())
	})

	t.Run("non existing", func(t *testing.T) {
		req := &pb.GetObjectRequest{
			Label: label,
			Key:   []byte("notexistingkey"),
		}

		_, err := api.Get(context.Background(), req)
		assert.Equal(t, db.ErrNotFound, err)
	})
}

func TestExistsObject(t *testing.T) {
	api, clean := getTestObjectAPI(t)
	defer clean()

	label, bufList := populateDB(t, api.db)

	for i := 0; i < len(bufList); i++ {
		key := fmt.Sprintf("testkey%d", i)
		t.Run(key, func(t *testing.T) {
			req := &pb.ExistsObjectRequest{
				Label: label,
				Key:   []byte(key),
			}

			resp, err := api.Exists(context.Background(), req)
			require.NoError(t, err)
			assert.True(t, resp.Exists, fmt.Sprintf("Key %s should exists", key))
		})
	}

	t.Run("non exists", func(t *testing.T) {
		req := &pb.ExistsObjectRequest{
			Label: label,
			Key:   []byte("nonexists"),
		}

		resp, err := api.Exists(context.Background(), req)
		require.NoError(t, err)
		assert.False(t, resp.Exists, fmt.Sprint("Key nonexists should not exists"))
	})
}

func TestDeleteObject(t *testing.T) {
	api, clean := getTestObjectAPI(t)
	defer clean()

	label, _ := populateDB(t, api.db)
	objMgr := manager.NewObjectManager(label, api.db)

	t.Run("valid", func(t *testing.T) {
		req := &pb.DeleteObjectRequest{
			Label: label,
			Key:   []byte("testkey1"),
		}

		_, err := api.Delete(context.Background(), req)
		require.NoError(t, err)

		exists, err := objMgr.Exists([]byte(req.Key))
		require.NoError(t, err)
		assert.False(t, exists)
	})

	// deleting a non existing object doesn't return an error.
	t.Run("non exists", func(t *testing.T) {
		req := &pb.DeleteObjectRequest{
			Label: label,
			Key:   []byte("nonexists"),
		}

		_, err := api.Delete(context.Background(), req)
		require.NoError(t, err)

		exists, err := objMgr.Exists([]byte(req.Key))
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestCheckObject(t *testing.T) {
	api, clean := getTestObjectAPI(t)
	defer clean()

	label, _ := populateDB(t, api.db)
	objMgr := manager.NewObjectManager(label, api.db)

	tt := []struct {
		name           string
		key            []byte
		expectedStatus manager.CheckStatus
	}{
		{
			name:           "valid",
			key:            []byte("testkey1"),
			expectedStatus: manager.CheckStatusOK,
		},
		{
			name:           "missing",
			key:            []byte("dontexsits"),
			expectedStatus: manager.CheckStatusMissing,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			status, err := objMgr.Check(tc.key)
			require.NoError(t, err, "failed to check status of %v", tc.key)
			assert.Equal(t, tc.expectedStatus, status)
		})
	}
}
