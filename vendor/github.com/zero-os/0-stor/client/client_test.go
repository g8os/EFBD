package client

import (
	"crypto/rand"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/zero-os/0-stor/client/lib/distribution"
	"github.com/zero-os/0-stor/client/lib/replication"
	"github.com/zero-os/0-stor/client/meta/embedserver"

	"github.com/zero-os/0-stor/client/lib/encrypt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/config"
	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/client/lib/compress"
	"github.com/zero-os/0-stor/server/storserver"
)

// func testGRPCServer(t *testing.T, n int) (storserver.StoreServer, *itsyouonline.Client, string, func()) {
func testGRPCServer(t testing.TB, n int) ([]storserver.StoreServer, func()) {
	servers := make([]storserver.StoreServer, n)
	dirs := make([]string, n)

	for i := 0; i < n; i++ {

		tmpDir, err := ioutil.TempDir("", "0stortest")
		require.NoError(t, err)
		dirs[i] = tmpDir

		server, err := storserver.NewGRPC(path.Join(tmpDir, "data"), path.Join(tmpDir, "meta"))
		require.NoError(t, err)

		_, err = server.Listen("localhost:0")
		require.NoError(t, err, "server failed to start listening")

		servers[i] = server
	}

	clean := func() {
		for _, server := range servers {
			server.Close()
		}
		for _, dir := range dirs {
			os.RemoveAll(dir)
		}
	}

	return servers, clean
}

func testIYOClient(t testing.TB) (*itsyouonline.Client, string, string, string) {
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

	return iyoClient, iyoOrganization, iyoClientID, iyoSecret
}

func TestRoundTripGRPC(t *testing.T) {
	etcd, err := embedserver.New()
	require.NoError(t, err, "fail to start embebed etcd server")
	defer etcd.Stop()

	servers, serverClean := testGRPCServer(t, 4)
	defer serverClean()

	_, org, id, secret := testIYOClient(t)

	shards := make([]string, len(servers))
	for i, server := range servers {
		shards[i] = server.Addr()
	}

	conf := config.Config{
		Organization: org,
		Namespace:    "testnamespace",
		Protocol:     "grpc",
		Shards:       shards,
		MetaShards:   []string{etcd.ListenAddr()},
		IYOAppID:     id,
		IYOSecret:    secret,
	}

	tt := []struct {
		name  string
		pipes []config.Pipe
	}{
		{
			"no-pipe",
			[]config.Pipe{},
		},
		{
			"pipe-compress",
			[]config.Pipe{
				{
					Name: "compress",
					Type: "compress",
					Config: compress.Config{
						Type: compress.TypeSnappy,
					},
				},
			},
		},
		{
			"pipe-encrypt",
			[]config.Pipe{
				{
					Name: "encrypt",
					Type: "encrypt",
					Config: encrypt.Config{
						Type:    encrypt.TypeAESGCM,
						PrivKey: "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
					},
				},
			},
		},
		{
			"pipe-replicattion-async",
			[]config.Pipe{
				{
					Name: "replication",
					Type: "replication",
					Config: replication.Config{
						Async:  true,
						Number: len(servers),
					},
				},
			},
		},
		{
			"pipe-replicattion-sync",
			[]config.Pipe{
				{
					Name: "replication",
					Type: "replication",
					Config: replication.Config{
						Async:  false,
						Number: len(servers),
					},
				},
			},
		},
		{
			"pip-distribution",
			[]config.Pipe{
				{
					Name: "distribution",
					Type: "distribution",
					Config: distribution.Config{
						Data:   3,
						Parity: 1,
					},
				},
			},
		},
		{
			"pip-compress-encrypt-distribution",
			[]config.Pipe{
				{
					Name: "compress",
					Type: "compress",
					Config: compress.Config{
						Type: compress.TypeSnappy,
					},
				},
				{
					Name: "encrypt",
					Type: "encrypt",
					Config: encrypt.Config{
						Type:    encrypt.TypeAESGCM,
						PrivKey: "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
					},
				},
				{
					Name: "distribution",
					Type: "distribution",
					Config: distribution.Config{
						Data:   3,
						Parity: 1,
					},
				},
			},
		},
	}

	for _, tc := range tt {

		t.Run(tc.name, func(t *testing.T) {

			conf.Pipes = tc.pipes
			c, err := New(&conf)
			require.NoError(t, err, "fail to create client")

			data := make([]byte, 1024*1024)
			_, err = rand.Read(data)
			require.NoError(t, err, "fail to read random data")

			// write data to the store
			key := []byte("testkey")
			meta, err := c.Write(key, data, nil, nil, nil)
			require.NoError(t, err, "fail to write data to the store")

			// validate metadata
			metaKey, err := meta.Key()
			require.NoError(t, err, "fail to read key from meta")

			// meta keys are different then normal when using distribution
			if !strings.Contains(tc.name, "distribution") {
				assert.Equal(t, key, metaKey, "Key in metadata is not the same")
			}

			metaShards, err := meta.GetShardsSlice()
			for _, shard := range metaShards {
				assert.Contains(t, metaShards, shard, "shards in metadata is not one of the shards configured in the client")
			}

			// read data back
			dataRead, err := c.Read(key)
			require.NoError(t, err, "fail to read data from the store")
			assert.Equal(t, data, dataRead, "data read from store is not the same as original data")
		})
	}
}

func BenchmarkDirectWriteGRPC(b *testing.B) {
	etcd, err := embedserver.New()
	require.NoError(b, err, "fail to start embebed etcd server")
	defer etcd.Stop()

	servers, serverClean := testGRPCServer(b, 1)
	defer serverClean()

	_, org, id, secret := testIYOClient(b)

	shards := make([]string, len(servers))
	for i, server := range servers {
		shards[i] = server.Addr()
	}

	conf := config.Config{
		Organization: org,
		Namespace:    "testnamespace",
		Protocol:     "grpc",
		Shards:       shards,
		MetaShards:   []string{etcd.ListenAddr()},
		IYOAppID:     id,
		IYOSecret:    secret,
	}

	for _, proto := range []string{"rest", "grpc"} {
		b.Run(proto, func(b *testing.B) {
			conf.Protocol = proto
			c, err := New(&conf)
			require.NoError(b, err, "fail to create client")

			data := make([]byte, 1024*1024)
			_, err = rand.Read(data)
			require.NoError(b, err, "fail to read random data")

			// write data to the store

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte("testkey")
				_, err := c.Write(key, data, nil, nil, nil)
				require.NoError(b, err, "fail to write data to the store")
			}
		})
	}
}
