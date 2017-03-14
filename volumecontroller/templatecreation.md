# Create a template

While any volume can be used as a template for a new volume, this document describes how to bootstrap the system with creating a volume from an ubuntu image.
``

1. Download an ubuntu qcow2 image and mount it
```
curl -O https://cloud-images.ubuntu.com/releases/16.04/release/ubuntu-16.04-server-cloudimg-amd64-disk1.img
```

2. Create a new volume

TODO

3. Convert the image using qemu-img

Replace the [VOLUMEID] in the following statement with the one returned from the rest call in 2 to create a new volume.
When the nbdserver is running on a tcp socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -O nbd nbd+tcp://[HOST]:[PORT]/[VOLUMEID]
```
When the nbdserver is running on a unix socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -O nbd nbd+unix:///[VOLUMEID]?socket=/tmp/nbd-socket
```
