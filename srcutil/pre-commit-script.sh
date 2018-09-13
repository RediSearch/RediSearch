#!/bin/bash
set -e

if [ -z "$GIT_WORK_TREE" ]; then
    echo "GIT_WORK_TREE not set!"
    exit 1
fi

cd $GIT_WORK_TREE
$GIT_WORK_TREE/srcutil/code_style.sh --dry-run
