# NBDServer for blocks stored in ARDB


## Test locally

Make sure you have an ardb server running on `localhost:16379`

```
go build && ./nbdserver -protocol tcp -address ":6666"`
```

Connect your nbdclient using localhost:6666:
```
sudo nbd-client -b 4096 -name default localhost 6666 /dev/nbd1
```

## Development

### Volumecontroller api changes

When the volumecontroller raml definition changed, regenerate the client and stub interface:
```
go generate
```
