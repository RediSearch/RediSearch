#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE yum update -y
$MODE yum groupinstall -y "Development Tools"
$MODE yum remove -y gcc # remove gcc 7
$MODE yum install -y wget git openssl11 openssl11-devel gcc10 gcc10-c++
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc10-gcc 60 --slave /usr/bin/g++ g++ /usr/bin/gcc10-g++
# make openssl11 the default openssl devel package
$MODE ln -s /usr/lib64/pkgconfig/libssl11.pc /usr/lib64/pkgconfig/libssl.pc
$MODE ln -s /usr/lib64/pkgconfig/libcrypto11.pc /usr/lib64/pkgconfig/libcrypto.pc
$MODE ln -s /usr/lib64/pkgconfig/openssl11.pc /usr/lib64/pkgconfig/openssl.pc
source install_cmake.sh $MODE
