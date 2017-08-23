#!/bin/bash

curl -O https://capnproto.org/capnproto-c++-0.6.1.tar.gz
ORIGDIR="$PWD"
tar zxf capnproto-c++-0.6.1.tar.gz && rm -rf capnproto-c++-0.6.1.tar.gz
cd capnproto-c++-0.6.1 || (echo "couldn't download capnproto-c++-0.6.0" && exit 1)
./configure
sudo make install
cd "$ORIGDIR" && sudo rm -rf capnproto-c++-0.6.1
