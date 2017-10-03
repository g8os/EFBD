package server

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/stor"
)

func TestMain(m *testing.M) {
	err := os.Setenv("STOR_TESTING", "true")
	if err != nil {
		fmt.Printf("error trying to set STOR_TESTING environment variable: %v", err)
		os.Exit(1)
	}

	defer func() {
		_ = os.Unsetenv("STOR_TESTING")
	}()

	os.Exit(m.Run())
}

func TestServerMsgSize(t *testing.T) {
	require := require.New(t)

	temp, err := ioutil.TempDir("", "0stor")
	require.NoError(err)

	mib := 1024 * 1024

	for i := 2; i <= 64; i *= 2 {
		t.Run(fmt.Sprintf("size %d", i), func(t *testing.T) {
			maxSize := i
			srv, err := New(path.Join(temp, "data"), path.Join(temp, "meta"), false, maxSize)
			require.NoError(err, "server should have been created")
			defer srv.Close()

			addr, err := srv.Listen("localhost:0")
			require.NoError(err, "server should have started listening")

			cl, err := stor.NewClient(addr, "testnamespace", "")
			require.NoError(err, "client should have been created")

			key := []byte("foo")

			bigData := make([]byte, (maxSize+10)*mib)
			_, err = rand.Read(bigData)
			require.NoError(err, "should have read random data")

			smallData := make([]byte, (maxSize/2)*mib)
			_, err = rand.Read(smallData)
			require.NoError(err, "should have read random data")

			err = cl.ObjectCreate(key, bigData, []string{})
			require.Error(err, "should have exeeded message max size")

			err = cl.ObjectCreate(key, smallData, []string{})
			require.NoError(err, "should not have exeeded message max size")

			exists, err := cl.ObjectExist(key)
			require.NoError(err, "object should exist")
			require.True(exists, "object should exists")

			obj, err := cl.ObjectGet(key)
			require.NoError(err, "should be able to read message")
			require.Equal(smallData, obj.Value)
		})
	}

}
