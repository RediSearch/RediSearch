#!/bin/bash
set -e

REPO_ROOT_DIR=$(dirname $(dirname $(dirname $(realpath "$0"))))

cd $REPO_ROOT_DIR
./code_style.sh --dry-run

exit $?


