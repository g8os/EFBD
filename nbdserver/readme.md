# NBD Server for blocks stored in ARDB

## Build instructions

Clone the code to your GOPATH:
```
go get -d github.com/g8os/blockstor/nbdserver
```

Build (totally static) the binary:
```
cd $GOPATH/src/github.com/g8os/blockstor/nbdserver
CGO_ENABLED=0
GOOS=linux
go build -a -ldflags '-extldflags "-static"' .
```

Optionally you can now create a flist for your NBD server so you can easily start a container on a G8OS with your NBD server. Below the steps.

First put 'nbdserver' binary somewhere alone:
```
mkdir /tmp/nbdserver
cp nbdserver /tmp/nbdserver
```

Then, using JumpScale:

```
REMOTE_ARDB_SERVER="IP address of an ARDB server"
kvs = j.servers.kvs.getRocksDBStore(name='flist', namespace=None, dbpath="/tmp/flist-nbdserver.db")
f = j.tools.flist.getFlist(rootpath='/tmp/nbdserver', kvs=kvs)
f.add('/tmp/nbdserver/')
f.upload("REMOTE_ARDB_SERVER", 16379)
```

And finally, pack your RocksDB database, and you're done:
```
cd /tmp/flist-nbdserver.db/
tar -cf ../flist-nbdserver.db.tar *
cd ..
gzip flist-nbdserver.db.tar
```


## More

For more verbose documentation see the [/docs](/docs) directory.

You'll find there following sections about the NBD Server:

- [NBD Server Introduction](/docs/nbd/nbd.md)
- [Building your NBD Server](/docs/nbd/building.md)
- [NBD Server Configuration](/docs/nbd/config.md)
- [Using your NBD Server](/docs/nbd/using.md)
