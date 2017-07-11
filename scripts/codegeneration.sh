#!/bin/bash

generate_and_check() {
    DIR=$1

    # Perform code generation and verify that the git repository is still clean,
    # meaning that any newly-generated code was added in this commit.
    go generate "$DIR"

    GITSTATUS=$(git status --porcelain)
    if [ -z "$GITSTATUS" ]; then
        exit 0
    fi

    # turns out that that there are uncomitted changes possible
    # in the generated code, exit with an error
    echo -e "changes detected, run 'go generate \"$DIR\"' and commit generated code in these files:\n"
    echo "$GITSTATUS"
    exit 1
}

generate_and_check ./tlog
generate_and_check ./docs/assets
