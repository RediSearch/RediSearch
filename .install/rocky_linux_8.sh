#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE dnf update -y

# Development Tools includes python11 and config-manager
$MODE dnf groupinstall "Development Tools" -yqq
# install pip
$MODE dnf install python3.11-pip -y

# powertools is needed to install epel
$MODE dnf config-manager --set-enabled powertools

# get epel to install gcc13
$MODE dnf install epel-release -yqq

$MODE dnf install -y gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ \
    gcc-toolset-13-libatomic-devel make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which rsync python3.11-devel clang

cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh

source install_cmake.sh $MODE
