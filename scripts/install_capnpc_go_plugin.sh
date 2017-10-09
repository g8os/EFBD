#!/bin/bash

# install the vendored go-capnpc plugin
# (required by capnp exe for generating Go code)
VENDOR_DIR="github.com/zero-os/0-Disk/vendor"
PLUGIN_DIR="zombiezen.com/go/capnproto2/capnpc-go"
echo "installing vendored capnpc-go plugin..."
go install -v "$VENDOR_DIR/$PLUGIN_DIR"
