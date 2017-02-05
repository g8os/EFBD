# EFBD
EFficient Boot Storage

# Problem
Openvstorage currently is not optimized for boot drive storage and for big data archives. 

# Solution

Make separate architecture for boot volume storage.
It should be:
* Deduplicated
* Fast 

# Proposed architectre

![EFBD Architecture](https://docs.google.com/drawings/d/1XH0dqAPFfoNBUaA4A4Si7HnhFU_0xP0wFrJFvWig5iI/pub?w=1149&h=634 "EFBD Architecture")

#### General overview
1. Virtual Drive uses EFBD
2. Each block of data (16kb) is hashed and stored in ARDB/Tarantool instances utilizing the hash of the data block as a key.
3. Metadata stored is separate ARDB instance.
4. As soon as data block is accessed EFBD, EFBD accesses metada and using metada redirects to actual data from ARDB instance corresponding to the metada.
5. For recovery and history purposes we keep all historical metadata in a separate ARDB instance. It should be possible to rebuild current state metadata from this instance. 

#### Hasing and encryption:

1. Each block of data is encrypted with its hash (original hash - encryption key).
2. Hash of the encrypted piece (derivative hash, storage key) is used as a key in ARDB storage.
3. First byte of the hash will determine which instance should store it [0-255]. In total there are 256 master instances in the network.
4. Metadata storage keeps original and derivative hashes. 

#### ARDB Deamon
1. Each instance should have slave backup with async replication. 256 master and 256 slave.
2. For each block we keep metadata in CAPNP format (propose better if possible) with a references to up to 10 current users of the data.
3. Block metada keeps back referneces to which device is uses the data.
4. If nobody references to the data block anymore its get deleted.
5. If more than 10 references exists - block is marked as permanent and no reference accounting needed anymore.

##### Metadata storage 
[Metadata structure draft](https://docs.google.com/a/greenitglobe.com/spreadsheets/d/13MmEJ0OPdlovPn54ZJ-vLDtlGKYWw9EmkhST5GY0Wtc/edit?usp=sharing)

#### Snapshots and history: 
1. All changes to sequence of hashes should be logged to archive storage.

