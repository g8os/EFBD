# Create a template

While any vdisk can be used as a template for a new vdisk, this document describes how to bootstrap the system with creating a vdisk from an ubuntu image.
``

1. Download an ubuntu qcow2 image and mount it
```
curl -O https://cloud-images.ubuntu.com/releases/16.04/release/ubuntu-16.04-server-cloudimg-amd64-disk1.img
```

2. Create a new vdisk

TODO

3. Convert the image using qemu-img

Replace the [VDISKID] in the following statement with the one returned from the rest call in 2 to create a new vdisk.
When the nbdserver is running on a tcp socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -O nbd nbd+tcp://[HOST]:[PORT]/[VDISKID]
```
When the nbdserver is running on a unix socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -O nbd nbd+unix:///[VDISKID]?socket=/tmp/nbd-socket
```

# Create a new vdisk from a template

Example vdiskcreation post data:
```
{
    "blocksize":4096,
    "deduped":true,
    "driver":"ardb",
    "readOnly":false,
    "size":20000000000,
    "storagecluster":"default",
    "templatevdisk":"roblede"
}
```
