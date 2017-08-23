## Compress

- Compress/decompress the input data.
- Supported compression algorithms:
    - [snappy](https://github.com/google/snappy)
    - gzip
    - [lz4](https://github.com/lz4/lz4)

## example

```golang
	payload := make([]byte, 4096*4096)
	for i := 0; i < len(payload); i++ {
		payload[i] = 100
	}

	conf := Config{
		Type: TypeSnappy,
	}

	// compress the payload and
	// write it to block.BytesBuffer buf
	buf := block.NewBytesBuffer()
	w, _ := NewWriter(conf, buf)
	resp := w.WriteBlock(payload)
	
	// compressed data = buf.Bytes()
	
	// decompress
	r, _ := NewReader(conf)
	decompressed, _ := r.ReadBlock(buf.Bytes())
```
