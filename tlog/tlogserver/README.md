# Go Tlog Server

## erasure coding

It use [isa-l](https://github.com/01org/isa-l) C library and [templexxx/reedsolomon](https://github.com/templexxx/reedsolomon) Go library for erasure coding.

Only one can be used at a time.

By default, the erasure coding is done in go.

When using the C isa-l library for the erasure coding, `-tags isal` needs be passed to go build.
And `GODEBUG=cgocheck=0` environment variable need to be set in order to run it.


**Build**

From this repo root directory
```
make tlogserver
```


**Usage**

Run it
```
./bin/tlogserver  -storage-addresses=127.0.0.1:16379 -k 16 -m 4
```

It starts tlog server that:
- listen on default listen address 0.0.0.0:11211
- need 16 data shards and 4 parity/coding shards -> total 20 ardb 
- first ardb address is `127.0.0.1:16379`, the seconds is in same IP but in port `16380`, the third in port `16381`, and so on...

To specify each ardb adddress, use array as address, example:`-storage-addresses=127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381` to specify 3 ardbs

Use tlog client as described in [client readme](../tlogclient/readme.md) to send transaction log to this tlog server.


**benchmark**
```
GODEBUG=cgocheck=0 go test -tags isal -bench=.
```
