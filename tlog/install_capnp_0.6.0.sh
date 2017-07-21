#!/bin/bash

# install capnp
echo "installing 0.6.0 capnp release from source..."
ORIGDIR="$PWD"
curl -O https://capnproto.org/capnproto-c++-0.6.0.tar.gz
tar zxf capnproto-c++-0.6.0.tar.gz && rm -f capnproto-c++-0.6.0.tar.gz
cd capnproto-c++-0.6.0 || (echo "couldn't download capnproto-c++-0.6.0" && exit 1)
./configure
sudo make install
cd "$ORIGDIR" && sudo rm -rf capnproto-c++-0.6.0

# install the vendored go-capnpc plugin
# (required by capnp exe for generating Go code)
VENDOR_DIR="github.com/zero-os/0-Disk/vendor"
PLUGIN_DIR="zombiezen.com/go/capnproto2/capnpc-go"
echo "installing vendored capnpc-go plugin..."
go install -v "$VENDOR_DIR/$PLUGIN_DIR"
