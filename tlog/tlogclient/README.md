# TLOG Client

- TLOG Client library is a client lib that works on GIG BLOCKSTOR (NBD Server)
- TLOG Client communicates with TLOG Server via dpdk stack.
- Communication between client and server use binary capnp stream (no RPC).

## Data send from the client to server:
```
Block:
  - volume ID: uint32
  - sequence : uint64
  - LBA :uint64
  - Size :uint64
  - crc32 :uint32
  - data :Data (16K)
  - timestamp :uint64
 ```
