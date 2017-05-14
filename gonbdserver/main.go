package main

import (
	"flag"

	"github.com/g8os/blockstor/gonbdserver/nbd"
)

// main() is the main program entry
//
// this is a wrapper to enable us to put the interesting stuff in a package
func main() {
	nbd.RegisterFlags()
	flag.Parse()
	nbd.Run(nil)
}
