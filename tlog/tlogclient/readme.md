# TLOG Client

- TLOG Client library is a client lib that works on GIG BLOCKSTOR (NBD Server)
- TLOG Client communicates with TLOG Server via dpdk stack.
- Communication between client and server use binary capnp stream (no RPC).

## Data send from the client to server:

```
- vdiskID : String
- Block:
  - sequence : uint64
  - lba :uint64
  - size :uint32
  - hash :Data (32B)
  - data :Data (16K)
  - timestamp :uint64
 ```
