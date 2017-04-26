#!/bin/bash

BUILD_DIR=/tmp/build_tlog
rm -rf $BUILD_DIR
mkdir $BUILD_DIR


sudo apt-get install build-essential  autoconf automake nasm yasm
# install isa-l
cd $BUILD_DIR
git clone https://github.com/01org/isa-l.git
cd isa-l
./autogen.sh
./configure
make
sudo make install

# install isa-l_crypto
cd $BUILD_DIR
git clone https://github.com/01org/isa-l_crypto.git
cd isa-l_crypto
./autogen.sh
./configure
make
sudo make install
