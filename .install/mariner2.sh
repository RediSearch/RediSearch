#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

$MODE tdnf install -yq build-essential ca-certificates gdb git libxcrypt-devel openssl-devel rsync tar unzip wget which xz python3-devel
