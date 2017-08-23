ARDB With Rocksdb
=================

**Compiling**

```
docker run --hostname rocksdb --name rocksdb -i -d -t ubuntu
docker attach rocksdb

apt-get update
apt-get install -y build-essential gcc libgflags-dev unzip zlib1g-dev libbz2-dev libzstd-dev wget vim libtool autotools-dev automake pkg-config net-tools iputils-ping
wget -c https://github.com/Hamdy/ardb/archive/0.9.zip
unzip 0.9.zip
cd ardb-0.9
storage_engine=rocksdb make
```

**Config file**

```
vim ardb.conf

```

**Configurations**
```
thread-pool-size              16

rocksdb.options               write_buffer_size=512M;max_write_buffer_number=16;min_write_buffer_number_to_merge=2;compression=kSnappyCompression;\
                              bloom_locality=1;memtable_prefix_bloom_size_ratio=0.1;\
                              block_based_table_factory={block_cache=512M;filter_policy=bloomfilter:10:true};\
                              create_if_missing=true;max_open_files=-1;rate_limiter_bytes_per_sec=50M  #-1 means max?

```

**Running**
```
./src/ardb-server ardb.conf

```

ARDB With FORESTDB
==================

**Compiling**

```
docker run --hostname forestdb --name forestdb -i -d -t ubuntu
docker attach forrestdb

apt-get update
apt-get install -y build-essential gcc wget vim net-tools iputils-ping libaio-dev cmake unzip libtool autotools-dev automake pkg-config
wget -c https://github.com/Hamdy/ardb/archive/0.9.zip
unzip 0.9.zip
cd ardb-0.9
storage_engine=forestdb make
```

**Config file**

```
vim ardb.conf

```

**Configurations**
```
thread-pool-size              16

rocksdb.options               chunksize=8,blocksize=4K

```

**Running**
```
./src/ardb-server ardb.conf

```



Benchmarking
============

```
docker run --hostname benchmarker --name benchmarker -i -d -t ubuntu
docker attach benchmarker

apt-get install make build-essential gcc python-pip python-tk
wget -c http://download.redis.io/releases/redis-3.2.9.tar.gz
tar -zvxf redis-3.2.9.tar.gz
cd redis-3.2.9
./make
make install
```

***Benchmarking using pure redis-benchmark***

```
redis-benchmark -h 172.17.0.3 -p 16379 -r 1000 -n 1000 -t get,set,lpush,lpop -P 16 -q --csv
```

***Benchmarking & export image using our tool***

- Enter the benchmarking directory
```
cd benchmark
```

- Install Python requirements
```
pip install -r requirements.pip

```

- Change configurations for the ARDB servers you want to test

```json
{
  "databases": {
    "rocksdb": {
      "host": "172.17.0.3",
      "port": 16379
    },
    "forestdb": {
      "host": "172.17.0.3",
      "port": 16379
    }
  },
  "operations": ["GET" ,"SET"],
  "keyspace_length": 10000000,
  "number_of_requests": 10000000,
  "number_of_pipelined_requests": 16,
  "quiet": true,
  "number_of_iterations_per_db": 5
}
```


- Run
```
python benchmarker.py
```

- Now find the (result.png) image in the current directory
