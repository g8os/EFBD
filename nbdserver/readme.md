# NBDServer for blocks stored in ARDB


## Test locally

Make sure you have an ardb server running on `localhost:16379`!

```
go build && ./nbdserver -protocol tcp -address ":6666"
```

Connect your nbdclient to the server running on `localhost:6666`:

```
sudo nbd-client -b 4096 -name default localhost 6666 /dev/nbd1
sudo mkfs.ext4 -b 4096 /dev/nbd1
sudo mount /dev/nbd1 /mnt/sharedvolume
```
