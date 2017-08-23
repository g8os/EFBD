# Replication

- A replicater takes a chunk of data and list of multiple outputs.
- Input chunk of data is then replicated on all the configured outputs.

## Replication in pipe

The replication can be added in the pipe with this config

```go
conf := Config {
	Async: true,	// if true, do the replication in async way
	Number: 3, 		// number of replications to create. 0 means to all servers
}
```

## Example

```go
	var writers []block.Writer
	numWriter := 10
	data := make([]byte, 4096)

	// create block writers which do the replication
	for i := 0; i < numWriter; i++ {
		writers = append(writers, block.NewBytesBuffer())
	}

	// create writer
	w := NewWriter(writers, Config{Async: async})

	// replicate
	resp := w.WriteBlock(data)
	assert.Nil(t, resp.Err)

	// check replicated data
	for i := 0; i < numWriter; i++ {
		buff := writers[i].(*block.BytesBuffer)
		if bytes.Compare(data, buff.Bytes()) != 0 {
			log.Fatal("invalid replicated data")
		}
	}
```
```
