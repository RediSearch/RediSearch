#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq
$MODE apt dist-upgrade -yqq
$MODE apt install -yqq software-properties-common

$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE add-apt-repository ppa:git-core/ppa -y
$MODE apt update
$MODE apt-get install -yqq build-essential git wget make gcc-10 g++-10 openssl libssl-dev
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 60 --slave /usr/bin/g++ g++ /usr/bin/g++-10

# Install Python 3.7
#$MODE add-apt-repository ppa:deadsnakes/ppa -y
$MODE apt-get update
$MODE apt-get install python3.7 python3.7-venv python3-venv python3-dev -y

echo "which python3: $(which python3)"
echo "which python3.6: $(which python3.6)"
echo "which python3.7: $(which python3.7)"

# Set python3 to point to python3.7
$MODE update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 2
$MODE update-alternatives --set python3 /usr/bin/python3.7

#$MODE apt-get install python3-venv -y

source install_cmake.sh $MODE
