## Encrypt

- encrypt/decrypt the input data.
- Supports Asymmetric & Symmetric algorithms
- Currently Supported encryption algorithms:
    - AES GCM

## Example
```go
	privKey := make([]byte, aesGcmKeySize)
	nonce := make([]byte, aesGcmNonceSize)
	rand.Read(privKey)
	rand.Read(nonce)

	conf := Config{
		Type:    TypeAESGCM,
		PrivKey: string(privKey),
		Nonce:   string(nonce),
	}
	
	plain := []byte("hello world")

	// encrypt
	buf := block.NewBytesBuffer()

	w, _ := NewWriter(buf, conf)

	resp := w.WriteBlock(plain)

	// decrypt
	r, _ := NewReader(conf)

	decrypted, err := r.ReadBlock(buf.Bytes())

	assert.Equal(t, plain, decrypted)

```
