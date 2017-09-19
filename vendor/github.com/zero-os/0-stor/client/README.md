# 0-stor client   [![godoc](https://godoc.org/github.com/zero-os/0-stor/client?status.svg)](https://godoc.org/github.com/zero-os/0-stor/client)

[Specifications](specs)

API documentation : [https://godoc.org/github.com/zero-os/0-stor/client](https://godoc.org/github.com/zero-os/0-stor/client)

## Supported protocols

- GRPC

## Motivation

- Building a **secure** & **fast** object store with support for big files

## Basic idea
- Splitting a large file into smaller chunks.
- Compress each chunk using [snappy](https://github.com/google/snappy)
- Encrypt each chunk using Hash(content)
    - Hash functions supported: [blake2](https://blake2.net/)
    - Support for symmetric encryption
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
- Format for the metadata:
    ```
	type Chunk struct {
		Size   uint64    # Size of the chunk in bytes
		Key    []byte    # key used in the 0-stor
		Shards []string
	}
	type Meta struct {
		Epoch     int64  # creation epoch
		Key       []byte # key used in the 0-stor
		EncrKey   []byte # Encryption key used to encrypt this file
		Chunks    []*Chunk # list of chunks of the files
		Previous  []byte   # Key to the previous metadata entry
		Next      []byte   # Key to the next metadata entry
		ConfigPtr []byte   # Key to the configuration used by the lib to set the data.
	}
    ```
**chunks**

Depending on the policy used to create the client, the data can be splitted into multiple chunks or not.  Which mean the metadata can be composed of minimum one up to n chunks.

Each chunks can then have one or multiple shards.

- If you use replication, each shards is the location of one of the replicate.
- If you use distribution, each shards is the location of one of the data or parity block.



**metadata linked list**

With `Previous` and `Next` fields in the metadata, we can build metadata linked list of sequential data
such as transaction log/history and block chain.
The linked list can then be used to walk over the data in forward or backward fashion.

Metadata linked list will be build if  user specify previous meta key when
calling `WriteWithMeta` or `WriteFWithMeta` methods.

## Getting started
- [Getting started](./cmd/zerostorcli/README.md)


## Now into some technical details!

**Pre processing of data**

- Client does some preprocessing on each chunk of data before sending them to 0stor
- This is achieved by configuring a policy during client creation
- Supported Data Preprocessing:
    - [chunker](./lib/chunker)
	- [compression](./lib/compress/README.md)
    - [Hasher](./lib/hash/README.md)
    - [encryption](./lib/encrypt/README.md)
    - [distribution / erasure coding](./lib/distribution/README.md)
    - [replication](./lib/replication/README.md)

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

## Using 0-stor client examples:


In below example, when we store the data, the data will be processed as follow:
plain data -> compress -> encrypt -> distribution/erasure encoding (which send to 0-stor server and write metadata)

When we get the data from 0-stor, the reverse process will happen:
distribution/erasure decoding (which read metdata & Get data from 0-stor) -> decrypt -> decompress -> plain data.

To run this example, you need to run:
- 0-stor server at port 12345
- 0-stor server at port 12346
- 0-stor server at port 12347
- etcd server at port 2379

```go
package main

import (
	"log"

	"github.com/zero-os/0-stor/client"
)

func main() {

	policy := client.Policy{
		Organization:           "labhijau",
		Namespace:              "thedisk",
		DataShards:             []string{"http://127.0.0.1:12345", "http://127.0.0.1:12346", "http://127.0.0.1:12347"},
		MetaShards:             []string{"http://127.0.0.1:2379"},
		IYOAppID:               "the_id",
		IYOSecret:              "the_secret",
		Compress:               true,
		Encrypt:                true,
		EncryptKey:             "ab345678901234567890123456789012",
		BlockSize:              4096,
		ReplicationNr:          0, // force to use distribution
		ReplicationMaxSize:     0, // force to use distribution
		DistributionNr:         2,
		DistributionRedundancy: 1,
	}
	c, err := client.New(policy)
	if err != nil {
		log.Fatal(err)
	}

	data := []byte("hello 0-stor")
	key := []byte("hi guys")

	// stor to 0-stor
	_, err = c.Write(key, data)
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

### Example using configuration file

```go
package main

import (
	"log"
	"os"

	"github.com/zero-os/0-stor/client"
)

func main() {
	f, err := os.Open("./config.yaml")
	if err != nil {
		log.Fatal(err)
	}


	policy, err := client.NewPolicyFromReader(f)
	if err != nil {
		log.Fatal(err)
	}

	client, err := client.New(policy)
	if err != nil {
		log.Fatal(err)
	}

	data := []byte("hello 0-stor")
	key := []byte("hello")
	refList := []string("ref-1")

	// stor to 0-stor
	_, err = client.Write(key, data, refList)
	if err != nil {
		log.Fatal(err)
	}

	// read data
	stored, storedRefList, err := client.Read(key)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("stored value=%v\n", string(stored))

}


```


## Configuration

Configuration file example can be found on [config.yaml](./cmd/cli/config.yaml).

## Libraries

This client some libraries that can be used independently.
See [lib](./lib) directory for more details.

## CLI

A simple cli can be found on [cli](./cmd/zerostorcli) directory.

## Daemon

There will be client daemon on [daemon](./cmd/daemon) directory.
