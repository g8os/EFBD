package main

// #include "snappy.h"
// #cgo LDFLAGS: -lsnappy
import "C"
import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"time"
	"unsafe"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/snappy"
	"zombiezen.com/go/capnproto2"
)

var (
	pools map[int]*redis.Pool
)

type config struct {
	K                int
	M                int
	firstObjStorPort int
	firstObjStorAddr string
	privKey          string
	vdiskID          string
	nonce            string // encryption nonce
}

func main() {
	var conf config
	var dumpContent bool // we need it to compare the result of C++ vs Go version

	flag.IntVar(&conf.K, "k", 4, "K variable of erasure encoding")
	flag.IntVar(&conf.M, "m", 2, "M variable of erasure encoding")
	flag.StringVar(&conf.firstObjStorAddr, "first-objstor-addr", "127.0.0.1", "first objstor addr")
	flag.IntVar(&conf.firstObjStorPort, "first-objstor-port", 16379, "first objstor port")
	flag.StringVar(&conf.privKey, "priv-key", "12345678901234567890123456789012", "priv key")
	flag.StringVar(&conf.vdiskID, "vdiskid", "1234567890", "virtual disk ID")
	flag.StringVar(&conf.nonce, "nonce", "37b8e8a308c354048d245f6d", "encryption nonce")
	flag.BoolVar(&dumpContent, "dump-content", false, "dump content")

	flag.Parse()

	log.Printf("k=%v, m=%v\n", conf.K, conf.M)

	pools = map[int]*redis.Pool{}
	initRedisPool(&conf)

	// get all pieces
	// no need for erasure decoding now because we don't have any data loss
	merged := getAllPieces(&conf)

	// decrypt
	decrypted := decrypt(merged, conf.privKey, conf.nonce)

	if dumpContent {
		if err := ioutil.WriteFile("go_decrypted", decrypted, 0666); err != nil {
			log.Fatal(err)
		}
	}

	// uncompress
	uncLen, err := snappy.DecodedLen(decrypted)
	if err != nil {
		log.Fatal(err)
	}
	uncompressed := make([]byte, uncLen)
	res := C.s_uncompress((*C.char)(unsafe.Pointer(&decrypted[0])),
		(C.size_t)(len(decrypted)),
		(*C.char)(unsafe.Pointer(&uncompressed[0])))

	// TODO : check the return value
	// we don't check it now because the return value indicate an error
	// but in fact it can be uncompressed correctly.
	// there might be something wrong in how we use it because we don't
	// get this issue in C in how we use it because we don't
	// get this issue in C+++
	log.Printf("unc len = %vres = %v\n", uncLen, res)

	//uncompressed = uncompressed[:res]

	/**
	* don't use this pure Go version because it doesn't work.
	uncompressed, err = snappy.Decode(uncompressed, decrypted)
	if err != nil {
		log.Fatalf("failed to decode:%v, decrypted len:%v", err, len(decrypted))
	}
	*/

	// decode capnp
	decodeCapnp(bytes.NewBuffer(uncompressed))
}

func decrypt(encrypted []byte, privKey, nonce string) []byte {
	decrypter, err := tlog.NewAESDecrypter(privKey, nonce)
	if err != nil {
		log.Fatalf("failed to create aes decrypter:%v", err)
	}

	plain, err := decrypter.Decrypt(encrypted)
	if err != nil {
		log.Fatalf("failed to decrypt:%v", err)
	}
	return plain
}

func getAllPieces(conf *config) []byte {
	num := conf.K

	// get last key
	rc := pools[0].Get()
	key, err := redis.Bytes(rc.Do("GET", "last_hash_"+conf.vdiskID))
	if err != nil {
		log.Fatalf("failed to get last key:%v", err)
	}

	// get pieces
	all := []byte{}
	for i := 0; i < num; i++ {
		rc := pools[i+1].Get()
		b, err := redis.Bytes(rc.Do("GET", key))
		if err != nil {
			log.Fatalf("failed to get piece :%v, err:%v", i, err)
		}
		fmt.Printf("data - len = %v\n", len(b))
		all = append(all, b...)
	}
	return all
}

func decodeCapnp(r io.Reader) {
	log.Println("decode capnp")
	msg, err := capnp.NewDecoder(r).Decode()
	if err != nil {
		log.Fatalf("failed to decode capnp : %v", err)
	}

	agg, err := schema.ReadRootTlogAggregation(msg)
	if err != nil {
		log.Fatalf("failed to read root tlog : %v", err)
	}

	name, err := agg.Name()
	if err != nil {
		log.Fatal(err)
	}
	prevHash, err := agg.Prev()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("name=%v, timestamp=%v, prevHash=%v", name, agg.Timestamp(), prevHash)

	blocks, err := agg.Blocks()
	if err != nil {
		log.Fatalf("failed to get blocks : %v", err)
	}

	for i := 0; i < int(agg.Size()); i++ {
		block := blocks.At(i)
		data, err := block.Data()
		if err != nil {
			log.Fatalf("failed to get data of block:%v, err:%v", i, err)
		}
		fmt.Printf("seq = %v, data=%v\n", block.Sequence(), string(data[:4]))
	}
}

func initRedisPool(conf *config) {
	for i := 0; i < conf.K+conf.M+1; i++ {
		addr := fmt.Sprintf("%v:%v", conf.firstObjStorAddr, conf.firstObjStorPort+i)
		pools[i] = &redis.Pool{
			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,
			Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
		}
	}
}
