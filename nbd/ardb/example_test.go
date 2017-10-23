package ardb_test

import (
	"fmt"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
	"github.com/zero-os/0-Disk/redisstub/ledisdb"
)

func ExampleBool() {
	server := ledisdb.NewServer()
	c, err := ardb.Dial(config.StorageServerConfig{
		Address: server.Address(),
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.Do("SET", "foo", 1)

	exists, err := ardb.Bool(c.Do("EXISTS", "foo"))
	if err != nil {
		panic(err)
	}

	fmt.Print(exists)
	// Output:
	// true
}

func ExampleError() {
	server := ledisdb.NewServer()
	c, err := ardb.Dial(config.StorageServerConfig{
		Address: server.Address(),
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	err = ardb.Error(c.Do("AUTH", "foo"))

	fmt.Print(err)
	// Output:
	// authentication failure
}

func ExampleBytes() {
	server := ledisdb.NewServer()
	c, err := ardb.Dial(config.StorageServerConfig{
		Address: server.Address(),
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.Do("SET", "foo", []byte{1, 2, 3})

	replyByteSlice, err := ardb.Bytes(c.Do("GET", "foo"))
	if err != nil {
		panic(err)
	}

	fmt.Print(replyByteSlice)
	// Output:
	// [1 2 3]
}

func ExampleOptBytes() {
	server := ledisdb.NewServer()
	c, err := ardb.Dial(config.StorageServerConfig{
		Address: server.Address(),
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	replyByteSlice, err := ardb.OptBytes(c.Do("GET", "foo"))
	if err != nil {
		panic(err)
	}

	fmt.Print(replyByteSlice)
	// Output:
	// []
}

func ExampleInt64() {
	server := ledisdb.NewServer()
	c, err := ardb.Dial(config.StorageServerConfig{
		Address: server.Address(),
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.Do("SET", "foo", 1234)

	replyInt64, err := ardb.Int64(c.Do("GET", "foo"))
	if err != nil {
		panic(err)
	}

	fmt.Print(replyInt64)
	// Output: 1234
}

func ExampleInt64ToBytesMapping() {
	server := ledisdb.NewServer()
	c, err := ardb.Dial(config.StorageServerConfig{
		Address: server.Address(),
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	hash := []byte("a_hash")
	c.Do("HSET", hash, 123, []byte{4, 5, 6})
	c.Do("HSET", hash, 789, []byte{10, 11, 12})

	replyInt64ByteMap, err := ardb.Int64ToBytesMapping(c.Do("HGETALL", hash))
	if err != nil {
		panic(err)
	}

	fmt.Println(replyInt64ByteMap[123])
	fmt.Println(replyInt64ByteMap[789])
	// Output:
	// [4 5 6]
	// [10 11 12]
}

func ExampleNewCluster() {
	server := ledisdb.NewServer()
	defer server.Close()

	cfg := config.StorageClusterConfig{
		Servers: []config.StorageServerConfig{
			config.StorageServerConfig{Address: server.Address()},
		},
	}

	// providing a nil dialer will make
	// the ardb cluster use a default dialer
	cluster, err := ardb.NewCluster(cfg, nil)
	if err != nil {
		panic(err)
	}

	_, err = cluster.Do(ardb.Command(command.Set, "answer", 42))
	if err != nil {
		panic(err)
	}

	reply, err := cluster.Do(ardb.Command(command.Get, "answer"))
	if err != nil {
		panic(err)
	}

	replyStr := string(reply.([]byte))
	fmt.Print(replyStr)
	// Output: 42
}
