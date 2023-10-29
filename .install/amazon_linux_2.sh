#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE yum update -y
$MODE yum groupinstall -y "Development Tools"
$MODE yum remove -y gcc # remove gcc 7
$MODE yum install -y wget git openssl-devel openssl gcc10 gcc10-c++
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc10-gcc 60 --slave /usr/bin/g++ g++ /usr/bin/gcc10-g++
source install_cmake.sh $MODE
