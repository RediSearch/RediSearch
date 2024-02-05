#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
ARCH=$([[ $(uname -m) == x86_64 ]] && echo x86_64 || echo noarch)
$MODE yum update -y
$MODE yum install -y https://packages.endpointdev.com/rhel/7/os/${ARCH}/endpoint-repo.${ARCH}.rpm
$MODE yum groupinstall -y "Development Tools"
$MODE yum -y install centos-release-scl


$MODE yum -y install openssl-devel openssl bzip2-devel libffi-devel wget which git sqlite sqlite-devel\
    devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-make devtoolset-11-libatomic-devel

source /opt/rh/devtoolset-11/enable
echo "gcc version: $(gcc --version)"

cp /opt/rh/devtoolset-11/enable /etc/profile.d/scl-devtoolset-11.sh
bash

echo "gcc version after bash: $(gcc --version)"
echo "libstc++ version: $(find / -name libstdc++.so.6*)"

wget https://www.python.org/ftp/python/3.9.6/Python-3.9.6.tgz
tar -xvf Python-3.9.6.tgz
cd Python-3.9.6
./configure --enable-optimizations
$MODE make altinstall
update-alternatives --install /usr/bin/python3 python3 `which python3.9` 2
# gcc

cd -
source install_cmake.sh $MODE
