#!/usr/bin/env bash
set -euf
find . -type d | grep -v 'vendor\|.git' | xargs -L 1 `which golint`