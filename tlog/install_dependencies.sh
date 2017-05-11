#!/bin/bash

BUILD_DIR=/tmp/build_tlog
rm -rf $BUILD_DIR
mkdir $BUILD_DIR

sudo apt-get install build-essential autoconf automake nasm yasm

# install isa-l
cd $BUILD_DIR || (echo "creating %BUILD_DIR failed" && exit 1)
git clone https://github.com/01org/isa-l.git
cd isa-l || (echo "cloning https://github.com/01org/isa-l.git failed" && exit 1)
./autogen.sh
./configure
make
sudo make install
