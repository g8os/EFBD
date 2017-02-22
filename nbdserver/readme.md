# NBDServer for deduped blocks stored in ARDB


## Test locally

Make sure you have an ardb server running on `localhost:16379`

```
go build && ./nbdserver`
```

Connect your nbdclient using localhost:6666:
```
sudo nbd-client -b 4096 -name default localhost 6666 /dev/nbd1
```

[Specifications for LBA lookups](https://github.com/g8os/blockstor/blob/master/nbdserver/specs/cachedmetadata.md)
