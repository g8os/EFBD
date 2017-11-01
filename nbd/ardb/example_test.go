package ardb_test

import (
	"fmt"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
	"github.com/zero-os/0-Disk/redisstub"
	"github.com/zero-os/0-Disk/redisstub/ledisdb"
)

func ExampleCommand() {
	// given that we have a cluster...
	cluster := cluster()

	// we can use a storage command
	// to /do/ any supported ARDB command
	cluster.Do(
		ardb.Command(command.Set, "answer", 42))

	// and we can do another command to get our data
	answer, _ := ardb.Int64(cluster.Do(
		ardb.Command(command.Get, "answer")))
	fmt.Println(answer)
	// Output:
	// 42
}

func ExampleCommands() {
	// given that we have a cluster...
	cluster := cluster()

	// we can use a group storage commands
	// to /do/ any multitude of supported ARDB commands
	replies, _ := cluster.Do(
		ardb.Commands(
			ardb.Command(command.HashSet, "numbers", "four", 4),
			ardb.Command(command.HashSet, "numbers", "two", 2),
			ardb.Command(command.HashValues, "numbers")))

	numbers, _ := ardb.Int64s(replies.([]interface{})[2], nil)
	fmt.Println(numbers)
	// Output:
	// [4 2]
}

func ExampleBool() {
	c := cluster()
	defer c.Close()

	action := ardb.Command(command.Set, "foo", 1)
	err := ardb.Error(c.Do(action))
	panicOnError(err)

	action = ardb.Command(command.Exists, "foo")
	exists, err := ardb.Bool(c.Do(action))
	panicOnError(err)

	fmt.Print(exists)
	// Output:
	// true
}

func ExampleError() {
	c := cluster()
	defer c.Close()

	// set a key without value
	action := ardb.Command(command.Set, 1)
	err := ardb.Error(c.Do(action))

	fmt.Print(err)
	// Output:
	// invalid command param
}

func ExampleBytes() {
	c := cluster()
	defer c.Close()

	action := ardb.Command(command.Set, "foo", []byte{1, 2, 3})
	err := ardb.Error(c.Do(action))
	panicOnError(err)

	action = ardb.Command(command.Get, "foo")
	replyByteSlice, err := ardb.Bytes(c.Do(action))
	panicOnError(err)

	fmt.Print(replyByteSlice)
	// Output:
	// [1 2 3]
}

func ExampleOptBytes() {
	c := cluster()
	defer c.Close()

	action := ardb.Command(command.Get, "foo")
	replyByteSlice, err := ardb.OptBytes(c.Do(action))
	panicOnError(err)

	fmt.Print(replyByteSlice)
	// Output:
	// []
}

func ExampleInt64() {
	c := cluster()
	defer c.Close()

	action := ardb.Command(command.Set, "foo", 1234)
	err := ardb.Error(c.Do(action))
	panicOnError(err)

	action = ardb.Command(command.Get, "foo")
	replyInt64, err := ardb.Int64(c.Do(action))
	panicOnError(err)

	fmt.Print(replyInt64)
	// Output: 1234
}

func ExampleInt64ToBytesMapping() {
	c := cluster()
	defer c.Close()

	hash := []byte("a_hash")
	action := ardb.Command(command.HashSet, hash, 123, []byte{4, 5, 6})
	err := ardb.Error(c.Do(action))
	panicOnError(err)
	action = ardb.Command(command.HashSet, hash, 789, []byte{10, 11, 12})
	err = ardb.Error(c.Do(action))

	action = ardb.Command(command.HashGetAll, hash)
	replyInt64ByteMap, err := ardb.Int64ToBytesMapping(c.Do(action))
	panicOnError(err)

	fmt.Println(replyInt64ByteMap[123])
	fmt.Println(replyInt64ByteMap[789])
	// Output:
	// [4 5 6]
	// [10 11 12]
}

func ExampleNewCluster() {
	servers, cfg := serversAndStorageConfig()
	defer servers.Close()

	// providing a nil dialer will make
	// the ardb cluster use a default non-pooled dialer
	cluster, err := ardb.NewCluster(cfg, nil)
	panicOnError(err)

	action := ardb.Command(command.Set, "answer", 42)
	err = ardb.Error(cluster.Do(action))
	panicOnError(err)

	action = ardb.Command(command.Get, "answer")
	reply, err := ardb.String(cluster.Do(action))
	panicOnError(err)

	fmt.Print(reply)
	// Output: 42
}

func ExampleNewCluster_withPool() {
	servers, cfg := serversAndStorageConfig()
	defer servers.Close()

	// providing a nil dialFunc will make
	// the pool use a default one
	pool := ardb.NewPool(nil)
	defer pool.Close()

	cluster, err := ardb.NewCluster(cfg, pool)
	panicOnError(err)

	action := ardb.Command(command.Set, "answer", 42)
	err = ardb.Error(cluster.Do(action))
	panicOnError(err)

	action = ardb.Command(command.Get, "answer")
	reply, err := ardb.String(cluster.Do(action))
	panicOnError(err)

	fmt.Print(reply)
	// Output: 42
}

func serversAndStorageConfig() (*ledisdb.Server, config.StorageClusterConfig) {
	s := ledisdb.NewServer()
	cfg := config.StorageClusterConfig{
		Servers: []config.StorageServerConfig{
			config.StorageServerConfig{Address: s.Address()},
		},
	}
	return s, cfg
}

func cluster() *redisstub.UniCluster {
	return redisstub.NewUniCluster(false)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
