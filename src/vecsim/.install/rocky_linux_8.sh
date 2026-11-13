#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE dnf update -y

# Development Tools includes config-manager
$MODE dnf groupinstall "Development Tools" -yqq

# powertools is needed to install epel
$MODE dnf config-manager --set-enabled powertools

# get epel to install gcc13
$MODE dnf install epel-release -yqq

$MODE dnf install -y gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ gcc-toolset-13-libatomic-devel  make valgrind wget git

cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh

source install_cmake.sh $MODE
