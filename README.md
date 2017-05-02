# G8OS Block Storage [![Build Status](https://travis-ci.org/g8os/blockstor.svg?branch=master)](https://travis-ci.org/g8os/blockstor)

The G8OS block storage allows to create and use block devices (vdisks) on top of the [G80S object storage](https://github.com/g8os/objstor).

A vdisk can be deduped, have various blocksizes and depending on the underlying storage cluster used, have different speed characteristics.

Make sure to have Golang version 1.8 or above installed!

Components:
* [NBD Server](nbdserver/readme.md)
    A Network Block Device server to expose the vdisks to virtual machines.
* [TLOG Server](cmd/tlogserver/README.md)
    A Transaction log server to record block changes
* [CopyVdisk CLI](cmd/copyvdisk/readme.md)
    A CLI to copy the metadata of a deduped vdisk

# Build for g8os
- Clone the code to your GOPATH:
```
go get -d github.com/g8os/blockstor/nbdserver
cd $GOPATH/src/github.com/g8os/blockstor/nbdserver
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
