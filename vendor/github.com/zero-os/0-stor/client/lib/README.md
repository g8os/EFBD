# client lib

Client library is a set of libraries that can all be chain together to create a pipeline
that will process data as it goes through.

[godoc](https://godoc.org/github.com/zero-os/0-stor/client/lib)

# Example

compress
```
	conf := compress.Config {
		Type: compress.TypeSnappy,
	}
	payload := make([]byte, 4096)
	buf := block.NewBytesBuffer()
	w, err := compress.NewWriter(conf, buf)
	if err != nil {
		return
	}

	resp := w.WriteBlock(payload)
	if resp.Err != nil {
		return
	}

	if resp.Meta != nil {
		// use the metadata
	}
```

Other packages share same API, so the usage is the same.

## Pipe Example

Chaining/piping example can be found on [pipe package](../pipe/README.md).
