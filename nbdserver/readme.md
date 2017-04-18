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

## Development

### Volumecontroller api changes

When the volumecontroller raml definition changed, regenerate the docs, client- and stub interface:

```
go generate
```

Dependencies:

+ Go 1.8 or above;
+ `npm i -g raml2html`: used for generation of HTML docs;
+ `go get -u github.com/Jumpscale/go-raml`: used to generate stubs and clients;
