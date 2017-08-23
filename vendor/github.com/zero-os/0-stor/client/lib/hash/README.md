# Hash

- hash the input data.
- Supported hashing algorithm:
    - sha256
    - blake2 256
    - md5

## Example

- Hasher
    ```go
        data := make([]byte, 4096)
        hasher, _ := NewHasher(Config{
            Type: TypeBlake2,
        })
        hasher.Hash(data)
    ```

- block.Writer
    ```go
        data := make([]byte, 4096)
        buf := block.NewBytesBuffer()
        w, _ := NewWriter(buf, Config{Type:TypeBlake2})
        w.WriteBlock(data)
    ```
