#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

# $MODE tdnf install -y --noplugins tar git ca-certificates
$MODE tdnf install -q -y build-essential git wget ca-certificates tar unzip \
                         rsync openssl-devel python3 python3-pip python3-devel \
                         which clang libxcrypt-devel
