# GIG TLOG 

Transaction Log

## TLOG Client

- TLOG Client library is a client lib that works on GIG BLOCKSTOR (NBD Server)
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
  - lba :uint64          # block lba
  - size :uint64         # data size
  - hash :Data           # hash of the data      
  - data :Data           # payload data
  - timestamp :uint64    # timestamp of the transaction
  - operation :uint8     # the type of the transaction
 ```

See the [README](tlogclient/readme.md) there for more details.

## TLOG Server

- TLOG Server store received log entries and store it in memmory
- After storing log entry it replies to the client on successfull transaction.
- after timeout or size of the aggregation is reached, we `Flush` it:
	- aggregate the entries
	- compress the aggregate
	- encrypt the compressed aggregate
	- erasure encode the encrypted aggregate
	- store each pieces of erasure encoded pieces to ardb in parallel way

- Ideal setup would be to spread erasure coded pieces on different ardb instances.
- Each instance is used to keep erasure coded part according to its index (erasure coded part index == ardb instance index)
- We keep only backward links in our blockchain of history. We will add separate forward lining structure later in case it will be needed for the speed of recovery


### Flush Settings

settings directly related to flush:
- flush-size: minimum number of blocks to be flushed
- flush-time: maximum time we can wait entries before flushing it.
- k : number of erasure encoded data pieces
- m : number of erasure encoded coding/parity pieces
- nonce: hex nonce used for encryption 
- priv-key: encryption private key

### Tlog Aggregation structure:
Tlog aggregation per vdisk
```
name (Text)          # unused now
size (uint64)        # number of blocks in this aggregation
timestamp (uint64)
vdiskID (uint32)     # vdisk ID
Blocks: List(Block)  
prev: Data           # hash of previous aggregation
```

The server code is in the [`tlogserver`](tlogserver/) directory, see the [README](tlogserver/README.md) there for more details.

## Code Generation

Cap'n Proto code (`schema/tlog_schema.capnp.go`) can be generated using `go generate`,
make sure to have capnp 0.6.0 installed by running `./install_capnp_0.6.0.sh`.

Always make sure to keep the generated code up to date with its relevant schema!

## More

More verbose documentation can be found in the [`/docs`](/docs) directory.
