#!/usr/bin/python

import os
import json
import errno
import shutil

from subprocess import call

# util function to copy anything
def copy_anything(src, dst):
    try:
        shutil.copytree(src, dst)
    except OSError as exc: # python >2.5
        if exc.errno == errno.ENOTDIR:
            shutil.copy(src, dst)
        else: raise

# define the absolute location of the Godeps dependency file
GO_SRC_DIR = os.path.join(os.environ["GOPATH"], "src")
ROOT_DIR = os.path.join(GO_SRC_DIR, "github.com", "zero-os", "0-Disk")
DEPS_FILE = os.path.join(ROOT_DIR, "Godeps", "Godeps.json")

# update all dependencies, one by one
with open(DEPS_FILE) as godeps_data:
    GODEPS = json.load(godeps_data)
    for DEP in GODEPS["Deps"]:
        DEP_PATH = DEP["ImportPath"]
        print "updating {}...".format(DEP_PATH)
        call(["go", "get", "-u", "-d", "-v", DEP_PATH])

# update godep and update all downloaded dependencies
call(["go", "get", "-u", "-v", "github.com/tools/godep"])
# update vendored deps
call(["godep", "update", "-v", "./..."])
# make sure all needed deps are also added
# and clean up any old stuff
call(["godep", "save", "-v", "./..."])

# copy non-go-dependencies manually
# yes, it's a hack, just like this entire file is one
NON_GO_DEPS = [
    "zombiezen.com/go/capnproto2/std/go.capnp",
    "zombiezen.com/go/capnproto2/capnpc-go"
]
VENDOR_DIR = os.path.join(ROOT_DIR, "vendor")

for rel_dep in NON_GO_DEPS:
    print "manually copying {}...".format(rel_dep)
    src = os.path.join(GO_SRC_DIR, rel_dep)
    dst = os.path.join(VENDOR_DIR, rel_dep)
    copy_anything(src, dst)

print "Done!"
