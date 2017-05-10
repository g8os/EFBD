# Building your NDB Server

The below documentation uses a Docker container with JumpScale 8.2 installed, as documented in the [Jumpscale/developer](https://github.com/Jumpscale/developer) GitHub repository.

First set following environment variables:
```
GIGHOME="/Users/$USER/gig"
ZEROTIER_NETWORK_ID="..."
```

Create a new Docker image and start a container, using the `js_builder_js82_zerotier.sh` script from the [Jumpscale/developer](https://github.com/Jumpscale/developer) GitHub repository:
```
curl -sL https://raw.githubusercontent.com/Jumpscale/developer/master/scripts/js_builder_js82_zerotier.sh | bash -s <your-ZeroTier-network-ID>
```

Or in case you already have the image, start a new container using the existing image:
```
docker rm --force js8
docker run --name js82 -h js82 -d --device=/dev/net/tun --cap-add=NET_ADMIN --cap-add=SYS_ADMIN -v ${GIGHOME}/zerotier-one/:/var/lib/zerotier-one/ -v ${GIGHOME}/code/:/opt/code/ -v ${GIGHOME}/data/:/optvar/data jumpscale/js82 /root/init.sh ${ZEROTIER_NETWORK_ID}
```

As a result you'll have a Docker container with JumpScale 8.2 installed and connected to your ZeroTier network.

Then start the SSH daemon in the container, so you can SSH into the container:
```
docker exec -t js82 bash -c "/usr/sbin/sshd"
```

Get the ZeroTier IP address your container got assigned, and save it in a environment variable:
```
ZEROTIER_IP_ADDRESS=`docker exec -t js82 bash -c "ip -4 addr show zt0 | grep -oP 'inet\s\d+(\.\d+){3}' | sed 's/inet //' | tr -d '\n\r'"`
```

For the next steps we'll SSH into your container over the ZeroTier network:
```
ssh root@$ZEROTIER_IP_ADDRESS
```

First we need to install Go into the Docker container. JumpScale makes that very easy:
```
jspython -c "from JumpScale import j; j.tools.cuisine.local.development.golang.install()"
```

Next:
```
mkdir -p /usr/local/go
export GOROOT=/opt/go/root/
export PATH=$PATH:$GOROOT/bin
export GOPATH=/opt/go/proj
```

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
