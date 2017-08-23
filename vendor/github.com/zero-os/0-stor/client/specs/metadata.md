# Metadata

The 0-stor-lib will produce metadata when putting file into the 0-stor.
These metadata will be stored in etcd.

During the retrieving of a file from the 0-stor, the lib will first contact etcd, get the metadata of the file, and then do the required call on the different 0-stor to rebuild the files from the blocks.

Capnp format for the metadata:
```capnp
struct Metadata {
    Size @0 :UInt64;
    # Size of the data in bytes
    Epoch @1 :UInt64;
    # creation epoch
    Key @2 :Data;
    # key used in the 0-stor
    EncrKey @3 :Data;
    # Encryption key used to encrypt this file
    Shard @4 :List(Text);
    # List of shard of the file. It's a url the 0-stor
    Previous @5 :Data;
    # Key to the previous metadata entry
    Next @6 :Data;
    # Key to the next metadata entry
    ConfigPtr @7 :Data;
    # Key to the configuration used by the lib to set the data.
}
```
