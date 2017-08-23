# 0-stor client   [![godoc](https://godoc.org/github.com/zero-os/0-stor/client?status.svg)](https://godoc.org/github.com/zero-os/0-stor/client)

[Specifications](specs)

API documentation : [https://godoc.org/github.com/zero-os/0-stor/client](https://godoc.org/github.com/zero-os/0-stor/client)

## Supported protocols

- REST (incomplete) : storing and retrieving data already supported
- GRPC (todo)

## Motivation

- Building a **secure** & **fast** object store with support for big files

## Basic idea
- Splitting a large file into smaller chunks.
- Compress each chunk using one of [snappy](https://github.com/google/snappy), [lz4](https://github.com/lz4/lz4), or [gzip](http://www.gzip.org/) algorithms
- Encrypt each chunk using Hash(content)
    - Hash functions supported, sha256, [blake2](https://blake2.net/), md5
    - Support for both symmetric & a symmetric encryption
- Save each part in different 0-stor server
- Replicate same chunk into different 0-stor servers
- Save metadata about chunks and where they're in etcd server or other metadata db
- Assemble smaller chunks for a file upon retrieval
    - Get chunks location from metadata server
    - Assemble file from chunks

## Important
- 0stor server is JUST a simple key/value store
- splitting, compression, encryption, and replication is the responsibility of client


**Features**

- [Erasure coding](http://smahesh.com/blog/2012/07/01/dummies-guide-to-erasure-coding/)

## Metadata

**Metadata format**
- Capnp format for the metadata:
    ```
    struct Metadata {
        Size @0 :UInt64; # Size of the data in bytes
        Epoch @1 :UInt64; # creation epoch
        Key @2 :Data; # key used in the 0-stor
        EncrKey @3 :Data; # Encryption key used to encrypt this file
        Shard @4 :List(Text); # List of shard of the file. It's a url the 0-stor
        Previous @5 :Data; # Key to the previous metadata entry
        Next @6 :Data; # Key to the next metadata entry
        ConfigPtr @7 :Data; # Key to the configuration used by the lib to set the data.
    }

    ```
**metadata linked list**

With `Previous` and `Next` fields in the metadata, we can build metadata linked list of sequential data
such as transaction log/history and block chain.
The linked list can then be used to walk over the data in forward or backward fashion.

Metadata linked list will be build if  user specify previous meta key when
calling `Write` or `WriteF` methods.

## Getting started
- [Getting started](./cmd/cli/README.md)


## Now into some technical details!

**Pre processing of data**

- Client does some preprocessing on each chunk of data before sending them to 0stor
- This is achieved through configurable pipeline
- Supported Data Preprocessing pipes:
    - [chunker](./lib/chunker) need to be in the start of write pipe or end of read pipe
    - [compression](./lib/compress/README.md)
    - [Hasher](./lib/hash/README.md)
    - [encryption](./lib/encrypt/README.md)
    - [distribution / erasure coding](./lib/distribution/README.md) either at the end of write pipe or start of read pipe
    - [replication](./lib/replication/README.md) @TODO

**backend pipelines**
- [0-stor rest/grpc client](./stor)
    - write final into store using either REST or GRPC using
    - will be added to the end of pipe automatically if there is no component to upload data to 0stor

**walk over the metadata**

Thanks to our metadata linked list, we can walk over the linked list in forward and backward mode
using `Walk` or `WalkBack` API.

Those APIs returns `channel` of `WalkResult` which then can be iterated.
The WalkResult consist of these fields:

- key in metadata
- the metadata
- the data stored on 0-stor server
- error if exist

It can be used for example to reconstruct the stored sequential data.

## Plain 0-stor client

We can use this client to store data to 0-stor server and retrieve it back without any pipe

Example

To run this example, you need to run:
- 0-stor server at port 12345
- 0-stor server at port 12346
- etcd server at port 2379

```go
package main

import (
	"log"

	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/config"
)

func main() {

	conf := config.Config{
		Organization: "labhijau",
		Namespace:    "thedisk",
		Protocol:     "rest",
		Shards:       []string{"http://127.0.0.1:12345", "http://127.0.0.1:12346"},
		MetaShards:   []string{"http://127.0.0.1:2379"},
		IYOAppID:     "the_id",
		IYOSecret:    "the_secret",
	}
	c, err := client.New(&conf)
	if err != nil {
		log.Fatal(err)
	}

	data := []byte("hello 0-stor")
	key := []byte("hi guys")

	// stor to 0-stor
	_, err = c.Write(key, data, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	// read the data
	stored, err := c.Read(key)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("stored data=%v\n", string(stored))
}


```
## 0-stor client with configurable pipe

In below example, we create 0-stor client with four pipes:

- compress
- encrypt
- distribution


When we store the data, the data will be processed as follow:
plain data -> compress -> encrypt -> distribution/erasure encoding (which send to 0-stor server and write metadata)

When we get the data from 0-stor, the reverse process will happen:
distribution/erasure decoding (which read metdata & Get data from 0-stor) -> decrypt -> decompress -> plain data.

To run this example, you need to run:
- 0-stor server at port 12345
- 0-stor server at port 12346
- etcd server at port 2379


### Example using configuration config object


```go
package main

import (
	"log"

	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/config"
	"github.com/zero-os/0-stor/client/lib/compress"
	"github.com/zero-os/0-stor/client/lib/distribution"
	"github.com/zero-os/0-stor/client/lib/encrypt"
)

func main() {
	compressConf := compress.Config{
		Type: compress.TypeSnappy,
	}
	encryptConf := encrypt.Config{
		Type:    encrypt.TypeAESGCM,
		PrivKey: "ab345678901234567890123456789012",
		Nonce:   "123456789012",
	}

	distConf := distribution.Config{
		Data:   1,
		Parity: 1,
	}

	conf := config.Config{
		Organization: "labhijau",
		Namespace:    "thedisk",
		Protocol:     "rest",
		IYOAppID:     "the_id",
		IYOSecret:    "the_secret",
		Shards:       []string{"http://127.0.0.1:12345", "http://127.0.0.1:12346"},
		MetaShards:   []string{"http://127.0.0.1:2379"},
		Pipes: []config.Pipe{
			config.Pipe{
				Name:   "pipe1",
				Type:   "compress",
				Config: compressConf,
			},
			config.Pipe{
				Name:   "pipe2",
				Type:   "encrypt",
				Config: encryptConf,
			},

			config.Pipe{
				Name:   "pipe3",
				Type:   "distribution",
				Config: distConf,
			},
		},
	}
	c, err := client.New(&conf)
	if err != nil {
		log.Fatal(err)
	}

	data := []byte("Hi you!")
	key := []byte("how low can you go")

	// stor to 0-stor
	_, err = c.Write(key, data, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	// get the value
	stored, err := c.Read(key)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("stored value=%v\n", string(stored))
}


```

### Example using configuration file

```go
package main

import (
	"log"
	"os"

	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/config"
)

func main() {
	f, err := os.Open("./config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	conf, err := config.NewFromReader(f)
	if err != nil {
		log.Fatal(err)
	}

	client, err := client.New(conf)
	if err != nil {
		log.Fatal(err)
	}

	data := []byte("hello 0-stor")
	key := []byte("hello")

	// stor to 0-stor
	_, err = client.Write(key, data, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	// read data
	stored, err := client.Read(key)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("stored value=%v\n", string(stored))

}


```


## Configuration

Configuration file example can be found on [config.yaml](./cmd/cli/config.yaml).
All configuration can be found on https://godoc.org/github.com/zero-os/0-stor/client/config

## Libraries

This client some libraries that can be used independently.
See [lib](./lib) directory for more details.

## CLI

A simple cli can be found on [cli](./cmd/cli) directory.

## Daemon

There will be client daemon on [daemon](./cmd/daemon) directory.
