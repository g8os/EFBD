# Using the NBD Server

Options:
- [Running locally](#running-locally)

<a id="running-locally"></a>
## Running locally



```
GIGHOME="/Users/$USER/gig"
ZEROTIER_NETWORK_ID="..."
```

Create a new Docker image and start a container:
```
curl -sL https://raw.githubusercontent.com/Jumpscale/developer/master/scripts/js_builder_js82_zerotier.sh | bash -s <your-ZeroTier-network-ID>
```

Or in case you already have the image, start a new container using the existing image:
```
docker rm --force js8
docker run --name js82 -h js82 -d --device=/dev/net/tun --cap-add=NET_ADMIN --cap-add=SYS_ADMIN -v ${GIGHOME}/zerotier-one/:/var/lib/zerotier-one/ -v ${GIGHOME}/code/:/opt/code/ -v ${GIGHOME}/data/:/optvar/data jumpscale/js82 /root/init.sh ${ZEROTIER_NETWORK_ID}


Then start the SSH daemon in the container:
```
/usr/sbin/sshd
```

For the next step you need to SSH into your container:
```
ssh root@ZERO-TIER-IP-ADDRESS
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

```
go get -d github.com/g8os/blockstor/nbdserver
```

```
cd $GOPATH/src/github.com/g8os/blockstor/nbdserver
CGO_ENABLED=0
GOOS=linux
go build -a -ldflags '-extldflags "-static"' .
```

Geert:

cd /opt/go/proj/src/github.com/g8os/resourcepool/api;

GOPATH=/opt/go/proj
GOROOT=/opt/go/root/

/opt/go/root/bin/go get -d ./...;

GOPATH=/opt/go/proj
GOROOT=/opt/go/root/
/opt/go/root/bin/go build -o /root/resourcepoolapiserver




GOPATH=$GIGHOME/


-> requires:
- ARDB
- disk image

-> you will run it in a container, so as part of the build process you will create a flist for it and upload it in an ARDB server, see: https://github.com/g8os/blockstor#build-for-g8os


See the README in the /nbdserver directory: https://github.com/g8os/blockstor/tree/master/nbdserver
-> TEST locally
  - require again a ARDB running and listening on on localhost:16379
  - ./nbdserver -protocol tcp -address ":6666"
  - uses an nbd-client


usage... -export... zie documentatie van geert: https://docs.greenitglobe.com/gig_products/vdc_gig_g8os/src/master/docs/create_vdisk_template.md#nbd-server

-> cd /opt/go/proj/src/github.com/g8os/blockstor/nbdserver
./nbdserver -export osboxes.org:ubuntu.16.04.2 -testardbs 172.30.208.208:26379,172.30.208.208:26379

-> this uses a NBD server connected to the central ARDB server; so no need to setup any ARDB yourself; see Jo's pointer in case you want to setup an own RDB server though


OR use the resource pool API: https://rawgit.com/g8os/resourcepool/1.1.0-alpha/raml/api.html#
  -> Create vdisk: https://rawgit.com/g8os/resourcepool/1.1.0-alpha/raml/api.html#vdisks_post
  -> this will automatically create/start an NDB server/container
  -> this requires however:
    - prior setup of storage cluster -> requiring a resource pool -> so first setup a resource pool -> then a storage cluster (using the resource cluster API)
    - templatevdisk, get one from the central ARDB server, see: https://docs.greenitglobe.com/gig_products/vdc_gig_g8os/src/master/docs/create_vdisk_template.md#nbd-server


pyclient... see script: https://docs.greenitglobe.com/g8os/demo/src/master/packet.net-resourcepool-10nodes-100vms/scripts/deployvms.py

the pyclient wrapes the rest API (so higher level)

pip install g8os-resourcepool -> doesn't work yet since not yet release -> so you need to pip install . in the pyclient directory: g8os/resourcepool/pyclient/

from g8os import resourcepool
from g8os.resourcepool import VdiskCreate, EnumVdiskCreateType, VMCreate, NicLink, EnumNicLinkType, VDiskLink

...

bd = VdiskCreate.create(blocksize=4096, id="%s_boot"%vm_name, size=bootdisksize, type=EnumVdiskCreateType.boot,
                                readOnly=False, storagecluster=storagecluster, templatevdisk=vmtemplate,
                                tlogStoragecluster=storagecluster)


...

dd = VdiskCreate.create(blocksize=4096, id="%s_data" % vm_name, size=datadisksize, type=EnumVdiskCreateType.db,
                        readOnly=False, storagecluster=storagecluster, templatevdisk=None,
                        tlogStoragecluster=storagecluster)

...


vm = VMCreate.create(cpu=1, disks=[boot, data], id=vm_name, memory=memory,
                             nics=[nic], systemCloudInit='', userCloudInit='')
