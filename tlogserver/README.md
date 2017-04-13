# Go Tlog server

## Run

```
GODEBUG=cgocheck=0 ./tlogserver
```

The `GODEBUG=cgocheck=0` is needed because we still have issue with cgo.

## benchmark
```
GODEBUG=cgocheck=0 go test -bench=.
```
