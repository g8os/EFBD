package client

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/zero-os/0-stor/client/meta/embedserver"
	"github.com/zero-os/0-stor/stubs"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/server/jwt"
	"github.com/zero-os/0-stor/server/storserver"
)

func TestMain(m *testing.M) {
	// configure the test jwt key
	pubKey, err := ioutil.ReadFile("../devcert/jwt_pub.pem")
	if err != nil {
		panic(err)
	}
	jwt.SetJWTPublicKey(string(pubKey))

	log.SetLevel(log.DebugLevel)
	os.Exit(m.Run())
}

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

func getTestClient(policy Policy) (*Client, error) {

	b, err := ioutil.ReadFile("../devcert/jwt_key.pem")
	if err != nil {
		panic(err)
	}

	key, err := jwtgo.ParseECPrivateKeyFromPEM(b)
	if err != nil {
		panic(err)
	}

	iyoCl, err := stubs.NewStubIYOClient("testorg", key)
	if err != nil {
		panic(err)
	}

	return newClient(policy, iyoCl)
}

func TestRoundTripGRPC(t *testing.T) {
	etcd, err := embedserver.New()
	require.NoError(t, err, "fail to start embebed etcd server")
	defer etcd.Stop()

	servers, serverClean := testGRPCServer(t, 4)
	defer serverClean()

	shards := make([]string, len(servers))
	for i, server := range servers {
		shards[i] = server.Addr()
	}

	policy := Policy{
		Organization: "testorg",
		Namespace:    "namespace1",
		Protocol:     "grpc",
		DataShards:   shards,
		MetaShards:   []string{etcd.ListenAddr()},
		IYOAppID:     "id",
		IYOSecret:    "secret",
	}

	tt := []struct {
		name      string
		BlockSize int

		ReplicationNr      int
		ReplicationMaxSize int

		DistributionNr         int
		DistributionRedundancy int

		Compress   bool
		Encrypt    bool
		EncryptKey string
	}{
		{
			name: "simple-write",
		},
		{
			name:      "chunks",
			BlockSize: 1024,
		},
		{
			name:     "compress",
			Compress: true,
		},
		{
			name:       "encrypt",
			Encrypt:    true,
			EncryptKey: "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
		},
		{
			name:               "replication",
			ReplicationNr:      len(shards),
			ReplicationMaxSize: 1024 * 1024,
		},
		{
			name:                   "distribution",
			ReplicationMaxSize:     1, //force to use distribution over replication
			DistributionNr:         3,
			DistributionRedundancy: 1,
		},
		{
			name:                   "chunks-distribution",
			BlockSize:              1024,
			ReplicationMaxSize:     1, //force to use distribution over replication
			DistributionNr:         3,
			DistributionRedundancy: 1,
		},
		{
			name:       "compress-encrypt",
			Compress:   true,
			Encrypt:    true,
			EncryptKey: "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
		},
		{
			name:       "chunk-compress-encrypt",
			BlockSize:  1024,
			Compress:   true,
			Encrypt:    true,
			EncryptKey: "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
		},
		{
			name:               "compress-encrypt-replication",
			Compress:           true,
			Encrypt:            true,
			EncryptKey:         "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
			ReplicationNr:      len(shards),
			ReplicationMaxSize: 1024 * 1024,
		},
		{
			name:                   "compress-encrypt-distribution",
			Compress:               true,
			Encrypt:                true,
			ReplicationMaxSize:     1, //force to use distribution over replication
			EncryptKey:             "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
			DistributionNr:         3,
			DistributionRedundancy: 1,
		},
		{
			name:               "chunks-compress-encrypt-replication",
			BlockSize:          1024,
			Compress:           true,
			Encrypt:            true,
			EncryptKey:         "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
			ReplicationNr:      len(shards),
			ReplicationMaxSize: 1024 * 1024,
		},
		{
			name:                   "chunks-compress-encrypt-distribution",
			BlockSize:              1024,
			Compress:               true,
			Encrypt:                true,
			ReplicationMaxSize:     1, //force to use distribution over replication
			EncryptKey:             "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
			DistributionNr:         3,
			DistributionRedundancy: 1,
		},
	}

	for _, tc := range tt {

		t.Run(tc.name, func(t *testing.T) {
			policy.BlockSize = tc.BlockSize
			policy.Compress = tc.Compress
			policy.Encrypt = tc.Encrypt
			policy.EncryptKey = tc.EncryptKey
			policy.DistributionNr = tc.DistributionNr
			policy.DistributionRedundancy = tc.DistributionRedundancy
			policy.ReplicationNr = tc.ReplicationNr
			policy.ReplicationMaxSize = tc.ReplicationMaxSize

			c, err := getTestClient(policy)
			require.NoError(t, err, "fail to create client")

			// data := []byte("1234567890")
			data := make([]byte, 1024*4)
			_, err = rand.Read(data)
			require.NoError(t, err, "fail to read random data")

			// write data to the store
			key := []byte("testkey")
			meta, err := c.Write(key, data)
			require.NoError(t, err, "fail to write data to the store")

			// validate metadata
			assert.Equal(t, key, meta.Key, "Key in metadata is not the same")
			// assert.EqualValues(t, len(data), meta.Size(), "size in the metadat doen't correspond with the size of the data")
			for _, chunk := range meta.Chunks {
				for _, shard := range chunk.Shards {
					assert.Contains(t, shards, shard, "shards in metadata is not one of the shards configured in the client")
				}
			}

			// b, err := json.Marshal(meta)
			// require.NoError(t, err)
			// fmt.Println(string(b))

			// read data back
			dataRead, err := c.Read(key)
			require.NoError(t, err, "fail to read data from the store")
			if bytes.Compare(data, dataRead) != 0 {
				t.Errorf("data read from store is not the same as original data")
				t.Errorf("len original: %d len actual %d", len(data), len(dataRead))
			}
			// assert.Equal(t, data, dataRead)
		})
	}
}

func TestMultipleDownload(t *testing.T) {

	// #test for https://github.com/zero-os/0-stor/issues/208

	etcd, err := embedserver.New()
	require.NoError(t, err, "fail to start embebed etcd server")
	defer etcd.Stop()

	servers, serverClean := testGRPCServer(t, 4)
	defer serverClean()

	shards := make([]string, len(servers))
	for i, server := range servers {
		shards[i] = server.Addr()
	}

	policy := Policy{
		Organization:           "testorg",
		Namespace:              "namespace1",
		Protocol:               "grpc",
		DataShards:             shards,
		MetaShards:             []string{etcd.ListenAddr()},
		IYOAppID:               "id",
		IYOSecret:              "secret",
		BlockSize:              1024000,
		Compress:               true,
		Encrypt:                true,
		EncryptKey:             "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
		ReplicationNr:          0,
		ReplicationMaxSize:     1, //force to use distribution over replication
		DistributionNr:         3,
		DistributionRedundancy: 1,
	}

	c, err := getTestClient(policy)
	require.NoError(t, err, "fail to create client")
	defer c.Close()

	data := make([]byte, 57446)

	_, err = rand.Read(data)
	require.NoError(t, err, "fail to read random data")
	key := []byte("testkey")

	_, err = c.Write(key, data)
	require.NoError(t, err, "fail write data")

	for i := 0; i < 100; i++ {
		result, err := c.Read(key)
		require.NoError(t, err, "fail read data")
		assert.Equal(t, data, result)
	}
}

func TestConcurentWriteRead(t *testing.T) {
	t.SkipNow()

	etcd, err := embedserver.New()
	require.NoError(t, err, "fail to start embebed etcd server")
	defer etcd.Stop()

	servers, serverClean := testGRPCServer(t, 4)
	defer serverClean()

	shards := make([]string, len(servers))
	for i, server := range servers {
		shards[i] = server.Addr()
	}

	policy := Policy{
		Organization:           "testorg",
		Namespace:              "namespace1",
		Protocol:               "grpc",
		DataShards:             shards,
		MetaShards:             []string{etcd.ListenAddr()},
		IYOAppID:               "id",
		IYOSecret:              "secret",
		BlockSize:              1024 * 64,
		Compress:               true,
		Encrypt:                true,
		EncryptKey:             "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
		ReplicationNr:          0,
		ReplicationMaxSize:     1, //force to use distribution over replication
		DistributionNr:         3,
		DistributionRedundancy: 1,
	}

	doReadWrite := func(i, size int) {
		c, err := getTestClient(policy)
		require.NoError(t, err, "fail to create client")
		defer c.Close()

		data := make([]byte, size)
		_, err = rand.Read(data)
		require.NoError(t, err, "fail to read random data")
		key := []byte(fmt.Sprintf("testkey-%d", i))

		_, err = c.Write(key, data)
		require.NoError(t, err, "fail write data")

		result, err := c.Read(key)
		require.NoError(t, err, "fail read data")
		assert.Equal(t, data, result, "data read is not same as data written")
	}

	// Seems we can't increased the number of concurent write more then around 32
	for concurent := 1; concurent <= 64; concurent *= 2 {
		for size := 1024; size < 1024*10; size *= 4 {
			name := fmt.Sprintf("Concurent client: %d - Size of the data: %d", concurent, size)
			t.Log(name)

			wg := &sync.WaitGroup{}
			wg.Add(10)
			start := time.Now()
			for i := 0; i < 10; i++ {
				go func(i int) {
					defer wg.Done()
					doReadWrite(i, size)
				}(i)
			}
			wg.Wait()
			end := time.Now()
			t.Logf("duration %d ms\n\n", (end.Sub(start).Nanoseconds() / 1000000))

		}
	}
}

func BenchmarkWriteFilesSizes(b *testing.B) {
	etcd, err := embedserver.New()
	require.NoError(b, err, "fail to start embebed etcd server")
	defer etcd.Stop()

	servers, serverClean := testGRPCServer(b, 4)
	defer serverClean()

	shards := make([]string, len(servers))
	for i, server := range servers {
		shards[i] = server.Addr()
	}

	policy := Policy{
		Organization:           "testorg",
		Namespace:              "namespace1",
		Protocol:               "grpc",
		DataShards:             shards,
		MetaShards:             []string{etcd.ListenAddr()},
		IYOAppID:               "id",
		IYOSecret:              "secret",
		BlockSize:              1024 * 1024, // 1MiB
		Compress:               true,
		Encrypt:                true,
		EncryptKey:             "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
		ReplicationNr:          0,
		ReplicationMaxSize:     1, //force to use distribution over replication
		DistributionNr:         3,
		DistributionRedundancy: 1,
	}

	c, err := getTestClient(policy)
	require.NoError(b, err, "fail to create client")
	defer c.Close()

	tt := []struct {
		Size int
	}{
		{1024},              // 1k
		{1024 * 4},          // 4k
		{1024 * 10},         // 10k
		{1024 * 1024},       // 1M
		{1024 * 1024 * 10},  // 10M
		{1024 * 1024 * 100}, // 100M
		{1024 * 1024 * 500}, // 500M
	}

	for _, tc := range tt {

		b.Run(fmt.Sprintf("%d", tc.Size), func(b *testing.B) {

			data := make([]byte, tc.Size)
			_, err = rand.Read(data)
			require.NoError(b, err, "fail to read random data")

			key := []byte("testkey")

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// write data to the store
				_, err := c.Write(key, data)
				require.NoError(b, err, "fail to write data to the store")
			}

			// read data back
			// dataRead, err := c.Read(key)
			// require.NoError(b, err, "fail to read data from the store")
			// if bytes.Compare(data, dataRead) != 0 {
			// 	b.Errorf("data read from store is not the same as original data")
			// 	b.Errorf("len original: %d len actual %d", len(data), len(dataRead))
			// }
		})
	}
}

// func BenchmarkDirectWriteGRPC(b *testing.B) {
// 	etcd, err := embedserver.New()
// 	require.NoError(b, err, "fail to start embebed etcd server")
// 	defer etcd.Stop()

// 	servers, serverClean := testGRPCServer(b, 1)
// 	defer serverClean()

// 	shards := make([]string, len(servers))
// 	for i, server := range servers {
// 		shards[i] = server.Addr()
// 	}

// 	conf := config.Config{
// 		Organization: "testorg",
// 		Namespace:    "testnamespace",
// 		Protocol:     "grpc",
// 		Shards:       shards,
// 		MetaShards:   []string{etcd.ListenAddr()},
// 		IYOAppID:     "id",
// 		IYOSecret:    "secret",
// 	}

// 	for _, proto := range []string{"rest", "grpc"} {
// 		b.Run(proto, func(b *testing.B) {
// 			conf.Protocol = proto
// 			c, err := getTestClient(&conf)
// 			require.NoError(b, err, "fail to create client")

// 			data := make([]byte, 1024*1024)
// 			_, err = rand.Read(data)
// 			require.NoError(b, err, "fail to read random data")

// 			// write data to the store

// 			b.ResetTimer()
// 			for i := 0; i < b.N; i++ {
// 				key := []byte("testkey")
// 				_, err := c.Write(key, data, nil, nil, nil)
// 				require.NoError(b, err, "fail to write data to the store")
// 			}
// 		})
// 	}
// }
