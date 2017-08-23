package main

import (
	"log"
	"os"

	"github.com/zero-os/0-stor/client/itsyouonline"
)

func main() {

	log.Printf("args=%v\n", os.Args)

	if len(os.Args) != 5 {
		log.Fatal("usage: ./main org clientID clientSecret namespace")
	}

	c := itsyouonline.NewClient(os.Args[1], os.Args[2], os.Args[3])
	log.Printf("resul err = %v\n", c.CreateNamespace(os.Args[4]))
}
