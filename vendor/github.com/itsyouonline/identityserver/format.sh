#!/usr/bin/env bash
gofmt -w $(find * -type d | grep -v '^vendor\|^.git\|^packaged\|^specifications')
