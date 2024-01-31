#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE dnf update -y

# Install python11
$MODE dnf groupinstall "Development Tools" -yqq
$MODE dnf config-manager --set-enabled powertools

$MODE dnf install epel-release -yqq


$MODE dnf install -y gcc-toolset-11-gcc gcc-toolset-11-gcc-c++ gcc-toolset-11-libatomic-devel make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which rsync

cp /opt/rh/gcc-toolset-11/enable /etc/profile.d/gcc-toolset-11.sh
echo "gcc version: $(gcc --version)"

source install_cmake.sh $MODE

$MODE dnf install python3.11-pip -y
