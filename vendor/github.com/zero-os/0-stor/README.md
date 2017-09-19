# 0-stor

[![Build Status](https://travis-ci.org/zero-os/0-stor.svg?branch=master)](https://travis-ci.org/zero-os/0-stor)

A Single device object store.


[link to group on telegram](https://t.me/joinchat/BwOvOw2-K4AN7p9VZckpFw)

## Components

## Server
The 0-stor server is a generic object store that prodive simple storage primitives, read, write, list, delete.

Underneat 0-stor uses [badger](https://github.com/dgraph-io/badger) as key value store. Badger allow to store the keys and the value in two different devices. Because of this separation the LSM tree of keys can most of the time stay in memory. Typically the keys could be kept in memory and depending on the use case, the values could be served from an SSD or HDD.

### Installation

Install the 0-stor server
```
go get -u github.com/zero-os/0-stor/cmd/zerostorserver
```

### How to run the server

## Running the server
Here are the options of the server:
```
   --debug, -d               Enable debug logging
   --bind value, -b value    Bind address (default: ":8080")
   --data value              Data directory (default: ".db/data")
   --meta value              Metadata directory (default: ".db/meta")
   --help, -h                show help
   --version, -v             print the version

```

Start the server with listening on all interfaces and port 12345
```shell
./zerostorserver --bind :12345 --data /path/to/data --meta /path/to/meta
```
## Client

The client is where all the logic of the communicaion with the 0-stor server lies.

The client provide some basic storage primitives to process your data before sending it to the 0-stor server :
- chunking
- compression
- encryption
- replication
- distribution/erasure coding

All of these primitives are configurable and you can decide how your data will be processed before beeing send to the 0-stor.

### Client API
Client API documentation is on godoc

[![godoc](https://godoc.org/github.com/zero-os/0-stor/client?status.svg)](https://godoc.org/github.com/zero-os/0-stor/client)

### Client CLI
You can find a CLI for the client in `cmd/zerostorcli`.

To install
```
go get -u github.com/zero-os/0-stor/cmd/zerostorcli
```


### More documentation

You can find more information about both component in the `/docs` folder of the repository.

* [Server docs](docs/README.md)
* [Client docs](client/README.md)
* [CLI docs](cmd/zerostorcli/README.md)
