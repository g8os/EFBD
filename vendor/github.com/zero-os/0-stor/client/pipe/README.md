# client pipe 

This package provide necessary helpers to create pipe from library writer/reader.

## Example

```go
    compressConf := compress.Config{
		Type: compressType,
	}
	encryptConf := encrypt.Config{
		Type:    encrypt.TypeAESGCM,
		PrivKey: "12345678901234567890123456789012",
		Nonce:   "123456789012",
	}
	hashConf := hash.Config{
		Type: hash.TypeBlake2,
	}

	conf := config.Config{
		Pipes: []config.Pipe{
			config.Pipe{
				Name:   "pipe1",
				Type:   "compress",
				Config: compressConf,
			},
			config.Pipe{
				Name:   "pipe2",
				Type:   "encrypt",
				Config: encryptConf,
			},
			config.Pipe{
				Name:   "pipe3",
				Type:   "hash",
				Config: hashConf,
			},
		},
	}

	finalWriter := block.NewBytesBuffer()

    // creates the pipe
	pw, err := NewWritePipe(&conf, finalWriter)
	if err != nil {
		return
	}
	
	data := make([]byte, 4096)
	
	// write the data
	resp := pw.WriteBlock(data)
```
