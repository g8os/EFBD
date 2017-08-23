# libg8stor
```bash
git clone https://github.com/zero-os/lib0stor
cd lib0stor
```

Prepare dependencies
```bash
rm -rf src/3rdparty/xxtea-c
git clone https://github.com/xxtea/xxtea-c.git src/3rdparty/xxtea-c
```

Install dependencies (all devel): `hiredis ssl snappy zlib python`

Compile library
```bash
cd src
make clean && make
```


Compile command-line tools
```bash
cd ../tools
make clean && make
```

Ensure library works with command-line
```bash
dd if=/dev/urandom of=/tmp/libstor-source bs=1M count=8
./g8stor-cli /tmp/libstor-source /tmp/libstor-verify
md5sum /tmp/libstor-*
```
