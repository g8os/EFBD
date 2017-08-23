
# 0-stor-lib

## process for uploading large file

- cut into parts (configurable size)
- each part encrypted using ??? hash of content
- input for uploading
   - see psuedo language
- etcd can optionally be used for storing the metadata
    - even as a double linked list, very handy for e.g. storing tlogs)
    - etcd is optional !!!

## pseudo language to use lib (example in python)

```python

policy=j.clients.0-stor.getPolicy()
policy.setShard(["http://192.168.66.10:3000/bpath/",...]) #21 locations (always at least 1 more than distr nr + redundancy)
policy.meta_cluster=["192.168.66.10","192.168.66.11","192.168.66.13"]
policy.blocksize=1024 #expressed in kb
policy.replication_maxsize=1024 #expressed in kb, if small than this then will be X way replication
policy.replication_nr = 3 #e.g. 3 way replication
policy.distribution_nr = 16 #means will cut into 16 EC pieces
policy.distribution_redundancy = 4# means will store 4 addiotional on top of 16
policy.compression = True
policy.encryption=True

policy.reservation_dataAccessToken="somethingdddddd"
policy.namespace="mynamespace" #namespace name as used on 0-stor

#we support no authentication for etcd for now, just open communication

cl=j.clients.0-stor.get(policy)

result,errors=cl.put(path=...,consumerid=...)
#when smaller than policy.replication_maxsize will choose random 3 (X) locations out of first shard untill success
#when bigger than replication: will use erasure coding over 20 from in this case 21 specified (random !)

print(result)
Out[8]:
{'encrkey': 'aabbccddeeffgghh',
 'key': 'bbccddeeffgghh',
 'shard': ['http://192.168.66.0:3000/bpath/',
  'http://192.168.66.1:3000/bpath/',
  'http://192.168.66.2:3000/bpath/',
  'http://192.168.66.3:3000/bpath/',
  'http://192.168.66.4:3000/bpath/',
  'http://192.168.66.5:3000/bpath/',
  'http://192.168.66.6:3000/bpath/',
  'http://192.168.66.7:3000/bpath/',
  'http://192.168.66.8:3000/bpath/',
  'http://192.168.66.9:3000/bpath/',
  'http://192.168.66.10:3000/bpath/',
  'http://192.168.66.11:3000/bpath/',
  'http://192.168.66.12:3000/bpath/',
  'http://192.168.66.13:3000/bpath/',
  'http://192.168.66.14:3000/bpath/',
  'http://192.168.66.15:3000/bpath/',
  'http://192.168.66.16:3000/bpath/',
  'http://192.168.66.17:3000/bpath/',
  'http://192.168.66.18:3000/bpath/',
  'http://192.168.66.19:3000/bpath/'],
 'size': 34324242,
 'epoch': 234234,
}

assert errors==[]
#errors is list of 0-stor servers which were down, if errors !=[] then a repair is needed to fix the store

result=cl.put(path=...,etcd=True,previous="keyOfPreviousMetaEntry", description="something",consumerid=... ) #will store the metadata in etcd

#when using etcd the metadata is stored in etcd as capnp info with 3 additional fields: previous, next & description
#when previous=... specified then the previous one is fetched, the next pointed to the new one, the release bumped (to make sure etcd does paxos well), and the new one is pointed back to the previous one, this creates a double linked list.

cl.get(metadata=result,path=...)

for item in cl.walk(start="keyOfPreviousMetaEntry",fromEpoch=234234,toEpoch=342344):
    print (item["description"])
    #description could be anything e.g. json encoded structural message which can be used to walk over history


for item in cl.walkBack(start="keyOfPreviousMetaEntry",fromEpoch=234234,toEpoch=342344):
    #same as before but now walk from last one to first one following the criteria

other functionality to be completed

cl.check(...)#will let us know if all parts of the erasure coded or replicated items are there

cl.repair(start="keyOfPreviousMetaEntry",fromEpoch=234234,toEpoch=342344,verify=True)
# will walk over the linked list and check if all items are there, basically do cl.check of each item
# if issue found then will re-encode and fix the missing part(s) and rewrite the metadata (release nr up)



```

## minimal example to restore info

```python

# with etcd

policy=j.clients.0-stor.getPolicy()
policy.meta_cluster=["192.168.66.10","192.168.66.11","192.168.66.13"]
policy.reservation_dataAccessToken="somethingdddddd"
policy.namespace="mynamespace" #namespace name as used on 0-stor

cl=j.clients.0-stor.get(policy)

cl.get(key,path)

#the metadata now comes out of the etcd cluster

```

```python

# without etcd

mdata = ... comes from other metadata store e.g. ardb ...

policy=j.clients.0-stor.getPolicy()
policy.reservation_dataAccessToken="somethingdddddd"
policy.namespace="mynamespace" #namespace name as used on 0-stor

cl=j.clients.0-stor.get(policy)

cl.get(metadata=mdata,path=...)
```

## remark about consumer id

- the client std does dedupe
- when uploading to 0-stor an exist needs to be done first, if it exists then only the consumerid is added
- when deleting then only the consumerid is removed !!!
- consumer id is a 16 byte key could be e.g. hash of path in NAS (multiple paths can point to same file)

## suggestions

- implement in golang as main language
- would do other languages by means of grpc to client which runs as daemon
    - e.g. zeroStorClient --daemon
    - then grpc allows setting of params (see above) and then set, get, walk, ...
    - https://husobee.github.io/golang/rest/grpc/2016/05/28/golang-rest-v-grpc.html

## todo
- lets do a test how much slower golang is compared to C implemente dusing ISA (intel)
