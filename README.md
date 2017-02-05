# EFBD
EFficient Boot Storage

# Problem
Openvstorage currently is not optimized for boot drive storage and for big data archives. 

# Solution

Make separate architecture for boot volume storage.
It should be:
* deduplicated
* Fast 

# Proposed architectre

![EFBD Architecture](https://github.com/g8os/EFBD/blob/master/EFBD%20Architecture.png "EFBD Architecture")

EFBD connects to a network of ARDB or Tarantool (decide what is better) instances.

- Each block of data (16kb) is hashed and stored in ARDB/Tarantool instances utilizing the hash of the data block as a key. 
- First byte of the hash will determine which instance should store it [0-255]. In total there are 256 master instances in the network.
- Eash instance should have slave backup with async replication. 256 master and 256 slave.
- For each block we keep metadata in CAPNP format with a references to up to 10 current users of the data.
- If nobody references to the data block anymore its get deleted.
- If more than 10 references exists - block is marked as permanent and no reference accounting needed anymore.

*1? Where and how do we keep sequence of hashes?



##Advancements:
###Snapshots and history: 
 - All changes to sequence of hashes should be logged to archive storage.
Encryption:
 - Each block of data is encypted using its hash (h) as a key. Then another hash (h') is taken from the encrypted block. h is stored in *1?. h' is used as a key for key-value storage. 

