package main

import (
	"flag"
	"log"
)

type config struct {
	K                int
	M                int
	flushSize        int
	flushTime        int
	privKey          string
	firstObjStorPort int
	firstObjStorAddr string
	bufSize          int
}

func main() {
	var port int

	var conf config

	flag.IntVar(&port, "port", 11211, "port to listen")
	flag.IntVar(&conf.flushSize, "flush-size", 25, "flush size")
	flag.IntVar(&conf.flushTime, "flush-time", 25, "flush time (seconds)")
	flag.IntVar(&conf.K, "k", 4, "K variable of erasure encoding")
	flag.IntVar(&conf.M, "m", 2, "M variable of erasure encoding")
	flag.StringVar(&conf.firstObjStorAddr, "first-objstor-addr", "127.0.0.1", "first objstor addr")
	flag.IntVar(&conf.firstObjStorPort, "first-objstor-port", 16379, "first objstor port")
	flag.StringVar(&conf.privKey, "priv-key", "12345678901234567890123456789012", "private key")
	flag.IntVar(&conf.bufSize, "buf-size", 16448, "buffer")

	flag.Parse()

	log.Printf("port=%v\n", port)
	log.Printf("k=%v, m=%v\n", conf.K, conf.M)

	// create server
	server, err := NewServer(port, &conf)
	if err != nil {
		log.Fatalf("failed to create server:%v\n", err)
	}

	server.Listen()
}
