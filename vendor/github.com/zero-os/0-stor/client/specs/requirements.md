# 0-stor-lib


This librairy is composed of multiple small compoments that can all be chain together to create a pipeline that will process data as it goes though.

The idea is that the clien can configure his pipeline as he wants, depending of its use case.

The different compoments present are:
- chunker : split data into smaller blocks of a fixed size.
- compression : compress or decompress the input
- encryption : cryp/decryp the input
- hash : generate different hashes
- replication : replicate one input onto multiple outputs
- erasure coding: see https://en.wikipedia.org/wiki/Erasure_code for more info about it.


## Compoments detail
This description of the components are written for a write case. Of course the opposite flow has to be also supported to be able to read the data back.

### Chunker
Creation of a chunker takes the chunk size.  
It returns an iterator that will yield a new chunk of data of the choosen size.  The block can then be sent to the rest of the pipeline

### Compression
Creation of compresser takes the type of compression used.
Supported :
- snappy
- lz4
- gzip

Depending on the compression algorithm used, some extra configuration need to be passed when creating the compresser.


### Encryption
Supports for both symetric and asymetric encryption.  
Creation of an encrypter takes the required key(s).

### Hash
Creation of hasher takes the type of hashing algorithm used.  
Supported:
- sha256
- blake2
- md5

The hasher compoment is a bit different from the other compoment since its outputs are not supposed to be send into the pipeline. Instead it generate the hash of the input data it gets and send the hash into a variable out of the pipeline to be reuse or even re-send into the pipeline as argument to other compoments.

### Replication
A replicater is created by taking one input and specifying multiple outputs.
All the data that comes in are replicated on all the configured outputs.


### Erasure coding:
Creation takes the erasure coding policy (k and n) and a list of outputs where to distribute the generated n blocks.  
Input data is splitted into n blocks, and then written to the ouputs.
This component is most probably going to be the last one of the pipeline.
