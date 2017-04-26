# Go Tlog server


## Using the isa-l library for erasure coding and aes encryption

**Build**

By default, the erasure coding is done in go.
When using the C isa-l library for the erasure coding, `-tags isal` needs be passed to go build.

**Run**

```
GODEBUG=cgocheck=0 ./tlogserver
```

The `GODEBUG=cgocheck=0` is needed because we still have issue with cgo.

**benchmark**
```
GODEBUG=cgocheck=0 go test -tags isal -bench=.
```
