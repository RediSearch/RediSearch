#!/bin/bash
set -e

if [ -z "$GIT_DIR" ]; then
    echo "GIT_DIR not set!"
    exit 1
fi

cd $GIT_DIR
$GIT_DIR/srcutil/code_style.sh --dry-run

exit $?


