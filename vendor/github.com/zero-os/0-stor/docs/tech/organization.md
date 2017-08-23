# Code organization

The 0-stor is composed of 3 layers:
- Database layer
- Manager layer
- Server interface

## Database layer
This is mostly located in the [db package](../../../server/db).  
The [db.go](../../../server/db/db.go) file define an interface that any key-value store need to implement to be usable by the 0-stor.

Currently the only implementation we have is using [badger](https://github.com/dgraph-io/badger) : see [badger package package](../../../server/db/badger)


## Manager layer
This is where the buisness logic of how we store the data in the key-value store. This layer also gives a unify API that the server interface can reuse.


The code is located in the [manager package](../../../server/manager)

## Service interface
We currently support 2 server interfaces:
- [rest](/server/api/rest)
- [grpc](/server/api/grpc)

These two interfaces are implemented in the [api package](../../../server/api)
