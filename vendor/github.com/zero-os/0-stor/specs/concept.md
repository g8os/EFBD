# Single Disk Stor (SDStor)

## Concept:
- services objects over network from a chosen data structure
- is backend object stor which can be used by high level client for storing files, directories, ...
- 2 interfaces:
    - rest based network server (using go-raml to generate)
    - grpc
- backend is [badger](https://github.com/dgraph-io/badger)
    - key and value can be written on two different location which enable use to put keys on a fast disk (ssd) and value on a slower disk. Since value log is always append, we don't loose too much performance even if we write in HDD
- we don't work with reference counter only, we need to keep list of consumers (reference counter to dangerous)

## Format of objects stored on disk
Structure used:
```
- 1 byte : Number of references stored, 0 means is deleted, 255 means permanent
- 160 * 16 bytes (2,56KB): list of consumers
    each consumer is represented with a 16 bytes id
- 4 bytes: CRC of payload behind
- payload: actual data, max size 1M
```

- consumer is a 16 byte string (this has no meaning for the 0-stor, its up to the user to give meaning to it e.g. IYO uid)
- special first byte meaning:
	- 0: no consumers, marked for deletion but is still there
	- 255: permanent = cannot be deleted

### Data Format send from the client:
The 0-stor is doesn't expect any specific data format from the client.
The knowledge of how the data are layout is the responsiility of the client. All client will be built using the [0-stor-lib](https://github.com/zero-os/0-stor-lib). The lib will be responsible to know how to process the data for write and read operations.

## Namespaces concept
We leverage ItsYou.online organization feature to create namespaces on the 0-stor.
There is not API to create namespaces, instead any member of an organization that has a name following the format: `myorg.0stor.mynamespace` will have access to the namespace called `myorg_0stor_mynamespace`.  
Since IYO organization name are unique, we avoid colision and problems in namespaces.

### ACL
The ACl also leverage IYO organizations.  
Imagine there is an organization with these sub organization:
```
myorg_0stor.mynamespace
myorg_0stor.mynamespace.write
myorg_0stor.mynamespace.read
myorg_0stor.mynamespace.delete
```
If a user is owner of an organization, he has all right (read/write/delete) on the corresponding namespace.  
If a user is not only member, he needs to be put into the proper sub organization(s) to get some rights. So if he only needs read access, the user need to be member of the `myorg_0stor.mynamespace.read`.

### Reference concept
The 0-stor expose a reference list concept to the user. To each object is attached a reference list.  
This reference list is fully managed by the user, so he needs to provide the list of reference id when creating an object, and updating this list according to its need. The 0-stor doesn't do anything with it.

### API

- [Raml specification](raml/sdstor.raml)
- [HTML rendered](https://htmlpreviewer.github.io/?./raml/sdstor.html)

## Tracking of usage and statistics

Per namespace track:
- nr of requests per hour
- nr of objects stored
- size of the data
