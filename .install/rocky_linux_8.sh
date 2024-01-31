#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE dnf update -y

# Install epel
$MODE dnf groupinstall "Development Tools" -y
$MODE dnf config-manager --set-enabled powertools
$MODE dnf install epel-release


$MODE dnf install -y gcc-toolset-11-gcc gcc-toolset-11-gcc-c++ make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which

source /opt/rh/gcc-toolset-11/enable
echo "gcc version: $(gcc --version)"

source install_cmake.sh $MODE

# Install python3.10 (latest version suggested by the default package manager is python3.6)
$MODE dnf install python39 -y
$MODE alternatives --set python `which python3.9`
update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1
