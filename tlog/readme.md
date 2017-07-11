# GIG TLOG 

Transaction Log

## TLOG Client

- TLOG Client library is a client lib that works on zero-os's zerodisk (NBD Server)
- TLOG Client communicates with TLOG Server via tcp and might be optimized by using dpdk stack.
- Communication between client and server use binary capnp stream (no RPC).

### Data send from the client to server:
Handshake request, send by client upon connection creation
```
HandshakeRequest
	version  :UInt32;
	vdiskID  :Text;
	firstSequence : UInt64;

```

Tlog block as tlog entry
```
Block:
  - sequence : uint64    # block sequence number
  - offset :uint64       # data offset
  - size :uint64         # data size
  - hash :Data           # hash of the data
  - data :Data           # payload data
  - timestamp :uint64    # timestamp of the transaction
  - operation :uint8     # the type of the transaction
 ```

See the [README](tlogclient/readme.md) there for more details.

## TLOG Server

The server code is in the [`tlogserver`](tlogserver/) directory, see the [README](tlogserver/README.md) there for more details.

## Code Generation

Cap'n Proto code (`schema/tlog_schema.capnp.go`) can be generated using `go generate`,
make sure to have capnp 0.6.0 installed by running `./install_capnp_0.6.0.sh`.

Always make sure to keep the generated code up to date with its relevant schema!

## More

More verbose documentation can be found in the [`/docs`](/docs) directory.
