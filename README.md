# G8OS Block Storage [![Build Status](https://travis-ci.org/g8os/blockstor.svg?branch=master)](https://travis-ci.org/g8os/blockstor)

The G8OS block storage allows to create and use block devices (volumes) on top of the [G80S object storage](https://github.com/g8os/objstor).

A volume can be deduped, have various blocksizes and depending on the underlying objectstor cluster used, have different speed characteristics.

Components:
* [Volume Controller](volumecontroller/readme.md)
    AYS to manage volumes. A rest api allows easy access from external systems.
* [NBD Server](nbdserver/readme.md)
    A Network Block Device server to expose the volumes to virtual machines.

# Build for g8os
- Clone the code to your GOPATH and `go get` the dependencies:
```
cd $GOPATH && mkdir -p github.com/g8os
cd github.com/g8os
git clone https://github.com/g8os/blockstor/
cd blockstor/nbdserver
go get .
```

- Build (totally static) the binary: `CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' .`
- Put that single binary somewhere alone: `mkdir /tmp/gonbdserver && cp nbdserver /tmp/gonbdserver`
- From JumpScale, create the flist:
```
kvs = j.servers.kvs.getRocksDBStore(name='flist', namespace=None, dbpath="/tmp/flist-gonbdserver.db")
f = j.tools.flist.getFlist(rootpath='/tmp/gonbdserver', kvs=kvs)
f.add('/tmp/gonbdserver/')
f.upload("remote-ardb-server", 16379)
```

- Pack your rocksdb database, and you're done:
```
cd /tmp/flist-gonbdserver.db/
tar -cf ../flist-gonbdserver.db.tar *
cd .. && gzip flist-gonbdserver.db.tar
```
