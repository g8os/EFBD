package main

import "github.com/zero-os/0-Disk/zeroctl/cmd"

func main() {
	// run the command in question (defined by the cli args)
	// see cmd dir for the possible commands and their behavior
	cmd.Execute()
}
