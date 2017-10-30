#!/bin/bash

generate_and_check() {
    DIR=$1
    echo "testing 'go generate' of \"$DIR\""

    # Perform code generation and verify that the git repository is still clean,
    # meaning that any newly-generated code was added in this commit.
    if go generate "$DIR"; then
        echo "succesfully generated \"$DIR\""
    else
        echo "failed to generate \"$DIR\""
        exit 1
    fi

    if GITSTATUS=$(git status --porcelain); then
        if [ -z "$GITSTATUS" ]; then
            echo "output of 'go generate \"$DIR\"' is up to date"
        else
            # turns out that that there are uncomitted changes possible
            # in the generated code, exit with an error
            echo -e "changes detected, run 'go generate \"$DIR\"' and commit generated code in these files:\n"
            echo "$GITSTATUS"
            exit 1
        fi
    else
        echo "failed get git status"
        exit 1
    fi
}

generate_and_check ./tlog

# there is no good way for now to have this cross-platform,
# produce reproducable results, and it doesn't impact (production) code either,
# so for now not doing it makes sense
#generate_and_check ./docs/assets
