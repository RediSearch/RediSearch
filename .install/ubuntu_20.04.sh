#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq
$MODE apt install -yqq software-properties-common
$MODE add-apt-repository ppa:deadsnakes/ppa -y
$MODE apt update -yqq

$MODE apt install -yqq wget make clang-format gcc python3 python3-venv python3-pip lcov git openssl libssl-dev \
    unzip rsync build-essential gcc-10 g++-10 curl libclang-dev

$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 60 --slave /usr/bin/g++ g++ /usr/bin/g++-10

# Install Python 3.12
$MODE apt -y install python3.12 python3.12-venv python3.12-dev

# Set python3 to point to python3.12
$MODE update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 2
