package ardb

import (
	"fmt"

	"github.com/zero-os/0-Disk/nbd/ardb/command"
)

func ExampleCommand() {
	// given that we have a cluster...
	cluster := cluster()

	// we can use a storage command
	// to /do/ any supported ARDB command
	cluster.Do(
		Command(command.Set, "answer", 42))

	// and we can do another command to get our data
	answer, _ := Int64(cluster.Do(
		Command(command.Get, "answer")))
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
		Commands(
			Command(command.HashSet, "numbers", "four", 4),
			Command(command.HashSet, "numbers", "two", 2),
			Command(command.HashValues, "numbers")))

	numbers, _ := Int64s(replies.([]interface{})[2], nil)
	fmt.Println(numbers)
	// Output:
	// [4 2]
}
