# G8OS Block Storage

The G8OS block storage allows to create and use block devices (volumes) on top of the [G80S object storage](https://github.com/g8os/objstor).

A volume can be deduped, have various blocksizes and depending on the underlying objectstor cluster used, have different speed characteristics.

Components:
* [Volume Controller](volumecontroller/readme.md)
    AYS to manage volumes. A rest api allows easy access from external systems.
* [NBD Server](nbdserver/readme.md)
    A Network Block Device server to expose the volumes to virtual machines.
