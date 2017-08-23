# Distribution / Erasure Coding
- Distribute chunks of data on cluster on a cluster of outputs/nodes
- Using [Erasure coding](http://smahesh.com/blog/2012/07/01/dummies-guide-to-erasure-coding/) policy (N, K) policy (data and parity number) and a list of outputs where to distribute the generated n blocks.
Input data is splitted into n blocks, and then written to the ouputs

- We've 2 types of distributors in client
    - Generic distribution
        - input/output are io.Reader/io.Writer
    - Stor Distribution
        - Save a chunks of data in 0stor cluster
        - Save metadata about all chunks of file and where they are in a configuration server such as [etcd server](https://github.com/coreos/etcd)
        - If we are using [chunker](../lib/chunker) pipeline, then we probably need to use  [metadata pipeline](../meta/README.md)
        in order to save all needed info/metadata to reconstruct file back from chunks into [etcd server](https://github.com/coreos/etcd)

## Example (Using Generic distribution)
- no pipelines

```go
	conf := Config{
		Data:   4,
		Parity: 2,
	}

	// create list of writers
	var writers []io.Writer
	for i := 0; i < conf.NumPieces(); i++ {
		buf := new(bytes.Buffer)
		writers = append(writers, buf)
	}
	
	data := make([]byte, 4096)

	// distribute
	d, _ := NewDistributor(writers, conf)


	_, err = d.Write(data)

	// restore
	var readers []io.Reader

	for i := 0; i < conf.NumPieces(); i++ {
		var reader io.Reader
		if i < conf.Parity {
			// simulate losing pieces here
			// we can lost up to `m` pieces
			reader = bytes.NewReader(nil)
		} else {
			reader = bytes.NewReader(buffs[i].Bytes())
		}
		readers = append(readers, reader)
	}

	r, _ := NewRestorer(readers, conf)

	decoded := make([]byte, len(data))

	n, _ := r.Read(decoded)

	if bytes.Compare(decoded, data) != 0 {
		log.Fatalf("restore failed")
	}
```
