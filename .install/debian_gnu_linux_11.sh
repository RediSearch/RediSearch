#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev python3-venv
source install_cmake.sh $MODE

echo INSTALLER="apt" >> $GITHUB_ENV
