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
$MODE apt-get install -yqq git wget make gcc-10 g++-10 openssl libssl-dev

# Install Python 3.10
$MODE add-apt-repository ppa:deadsnakes/ppa -y
$MODE apt install python3.10 python3.10-venv -y
# Set python3 to point to python3.10
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 1
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 2
sudo update-alternatives  --set python /usr/bin/python3.10

$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 60 --slave /usr/bin/g++ g++ /usr/bin/g++-10
source install_cmake.sh $MODE
