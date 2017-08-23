## Installation

The server is go-getable :
```
go get -u github.com/zero-os/0-stor/server
```
This will create `$GOPATH/bin/server`


Since server is not really explicty the recommended method to build is to use the makefile at the root of the repository

```shell
make server
```
This will create `$GOPATH/github.com/zero-os/0-stor/bin/zerostorserver`


## Running the server
```
NAME:
   zerostorserver - Generic object store used by zero-os

USAGE:
   server [global options] command [command options] [arguments...]

VERSION:
   0.0.1

DESCRIPTION:
   Generic object store used by zero-os

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --debug, -d               Enable debug logging
   --bind value, -b value    Bind address (default: ":8080")
   --data value              Data directory (default: ".db/data")
   --meta value              Metadata directory (default: ".db/meta")
   --interface value         type of server, can be rest or grpc (default: "rest")
   --help, -h                show help
   --version, -v             print the version

```

Start the server with REST listening on all interfaces and port 12345
```shell
./zerostorserver --bind :12345 --data /path/to/data --meta /path/to/meta --interface rest
```

Start the server with grpc listening on all interfaces and port 12345
```shell
./zerostorserver --bind :12345 --data /path/to/data --meta /path/to/meta --interface grpc
```