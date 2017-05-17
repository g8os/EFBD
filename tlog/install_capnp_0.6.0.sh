#!/bin/bash

# install capnp
ORIGDIR="$PWD"
curl -O https://capnproto.org/capnproto-c++-0.6.0.tar.gz
tar zxf capnproto-c++-0.6.0.tar.gz && rm -f capnproto-c++-0.6.0.tar.gz
cd capnproto-c++-0.6.0 || (echo "couldn't download capnproto-c++-0.6.0" && exit 1)
./configure
sudo make install
cd "$ORIGDIR" && sudo rm -rf capnproto-c++-0.6.0

# get go-capnp dependencies and util
go get -u -t zombiezen.com/go/capnproto2/...
# make sure to checkout the correct commit (as to version lock it)
cd "$GOPATH/src/zombiezen.com/go/capnproto2" && \
    git checkout d0d6fcbc1707dad418661921f3fb72174e8b0ddc
cd "$ORIGDIR" || (echo "failed to go back to $ORIGDIR" && exit 1)
