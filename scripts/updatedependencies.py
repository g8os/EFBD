#!/usr/bin/python

import os
import json

from subprocess import call

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

print "Done!"
