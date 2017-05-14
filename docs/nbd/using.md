# Using your NBD Server

This documentation supplements the usage instructions in the [README](/nbdserver/readme.md) of the [`nbdserver`](/nbdserver) source directory.

Options:
- [Using with local ARDB server](#local-ardb)
- [Using an ARDB Server running in a container on a single G8OS node](#ardb-container)
- [Using with a G8OS Storage Pool](#storage-pool)
- [Using the Python Client](#python-client)

<a id="local-ardb"></a>
## Local ARDB Server

Steps:
- [Setup an ARDB Server](#ardb-setup)
- [Check the configuration](#nbd-config)
- [Launch the NBD Server](#launch-nbd)
- [Test with nbd-client](nbd-client)
- [Converting an image](#convert-image)

<a id="ardb-setup"></a>
### Setup your local ARDB Server

To host a master template we first need to setup an [ARDB server](https://github.com/yinqiwen/ardb).

Get the source from GitHub:
```
git clone ...
```

Compile:
```
make ...
```

Check the ARDB configuration in `ardb.conf`:
```
https://github.com/yinqiwen/ardb/blob/0.9/ardb.conf
```

Start the ARDB server:
```
ardb-server
```

Make will have your ARDB server running, listening to `localhost:16379`.

<a id="nbd-config"></a>
### NBD Server Configuration

See: [NBD Server Configuration](config.md)

This leaves the NBD server running listening on standard unix socket at `unix:/tmp/nbd-socket` using our ARDB server for both metadata and data storage.

<a id="launch-nbd"></a>
### Launch the NBD Server

First you need your NBD Server, see the build instructions in [Building your NBD Server](building.md).

In order to run the `nbdserver` execute:
```
./nbdserver -protocol tcp -address ":6666"
```

<a id="nbd-client"></a>
### Test with nbd-client](nbd-client)

Connect your `nbd-client` to the server running on `localhost:6666`:

```
sudo nbd-client -b 4096 -name default localhost 6666 /dev/nbd1
sudo mkfs.ext4 -b 4096 /dev/nbd1
sudo mount /dev/nbd1 /mnt/sharedvolume
```

<a id="convert-image"></a>
### Converting an image

This will actually to copy our standard qcow2, img or vdi template file into ARDB.

Converting an image using qemu-img to insert an image in the NBD server:

```
apt-get install qemu-utils
```

When the NBD server is running on a TCP socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -O nbd nbd+tcp://[HOST]:[PORT]/[VDISKID]
qemu-img convert -p -n /optvar/data/images/ubuntu.16.04.2.img -O nbd nbd+tcp://localhost:6666/myvdisk
```

When the NBD server is running on a Unix socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -q nbd nbd+unix:///[VDISKID]?socket=/tmp/nbd-socket
```

<a id="ardb-container"></a>
## Using an ARDB Server running in a container on a single G8OS node

@todo


<a id="storage-pool"></a>
## Using with a G8OS Storage Pool

In case you don't have a storage pool available yet, create one using the Resource Pool API, as documented in [G8OS Storage Cluster](https://github.com/g8os/resourcepool/blob/master/docs/storagecluster/storagecluster.md)

This requires of course G8OS Resource Pool, which you can setup as documented in [Resource Pool Development Setup](https://github.com/g8os/resourcepool/blob/master/docs/setup/dev.md)

Once you have your storage cluster available you can:
- Manually setup a NBD server connecting, as documented above
- Or create a vdisk through the G8OS Resource Pool API, which will automatically launch a NBD container in the Resource Pool

For the second option, you'll again use the Resource Pool API: https://rawgit.com/g8os/resourcepool/1.1.0-alpha/raml/api.html

One of the arguments for creating a vdisk is `templatevdisk`, get one from the central ARDB server, documented in [Creating vdisk Templates](vdisktemplate.md)

<a id="python-client"></a>
## Using the Python Client

The Python client `pyclient` basically wraps the G8OS Resource Pool API.

First:
```
pip install g8os-resourcepool
```

Or if from `g8os/resourcepool/pyclient/` directory:
```
cd g8os/resourcepool/pyclient/
pip install .
```

See the example demo script: https://docs.greenitglobe.com/g8os/demo/src/master/packet.net-resourcepool-10nodes-100vms/scripts/deployvms.py
