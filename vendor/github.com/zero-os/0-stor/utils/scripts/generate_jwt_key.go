package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"os"
)

var (
	keySize = flag.Int("size", 2048, "size of the key")
	out     = flag.String("out", "", "destination of the generate key. If empty output on stdout")
)

func diep(msg string) {
	fmt.Fprintf(os.Stderr, "%v", msg)
	os.Exit(1)
}

func main() {
	flag.Parse()

	b := make([]byte, *keySize)
	_, err := rand.Read(b)
	if err != nil {
		diep(err.Error())
	}

	var output io.Writer
	if *out == "" {
		output = os.Stdout
	} else {
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0660)
		if err != nil {
			diep(err.Error())
		}
		defer f.Close()
		output = f
	}

	if _, err := fmt.Fprintf(output, "%x", b); err != nil {
		diep(err.Error())
	}
}
