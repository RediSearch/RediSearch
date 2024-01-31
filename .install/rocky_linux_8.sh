#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE dnf update -y
$MODE dnf install -y gcc gcc-c++ gcc-toolset-10-gcc gcc-toolset-10-gcc-c++ make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which

source /opt/rh/gcc-toolset-10/enable
source install_cmake.sh $MODE

# Install python3.10 (latest version suggested by the default package manager is python3.6)
$MODE dnf install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm -y
$MODE dnf config-manager --set-enabled powertools
$MODE dnf install epel-release
$MODE dnf install python38 -y
