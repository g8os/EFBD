# Chunker

Chunker returns an iterator that will yield a new chunk of data of the choosen size.
The block can then be sent to the rest of the pipeline.

## Example

```
	conf := Config{
		ChunkSize: 10,
	}

	data := make([]byte, 99)

	c := NewChunker(data, conf)
	for c.Next() {
		val := c.Value()
		fmt.Printf("val=%v\n", val)
	}

``
