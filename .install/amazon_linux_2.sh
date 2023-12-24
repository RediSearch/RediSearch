#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE yum update -y
$MODE yum groupinstall -y "Development Tools"
$MODE yum remove -y gcc # remove gcc 7
$MODE yum install -y wget git gcc10 gcc10-c++ python3
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc10-gcc 60 --slave /usr/bin/g++ g++ /usr/bin/gcc10-g++
# Install 'openss11' and make it the default so we will use it when linking.
# Currently it is commented out and handled on 'sbin/setup'. This is because
# 'sbin/setup' installs 'openssl-devel' which conflicts with 'openssl11-devel'.
# So if we install 'openssl11-devel' here we will cause 'sbin/setup' to fail.
# Once we remove the system setup we can uncomment those lines and install 'openssl11' and 'openssl11-devel'.
# When we do this we should also remember to remove the 'openssl' and 'openssl-devel' two lines above.
$MODE yum install -y openssl11 openssl11-devel
$MODE ln -s /usr/lib64/pkgconfig/libssl11.pc /usr/lib64/pkgconfig/libssl.pc
$MODE ln -s /usr/lib64/pkgconfig/libcrypto11.pc /usr/lib64/pkgconfig/libcrypto.pc
$MODE ln -s /usr/lib64/pkgconfig/openssl11.pc /usr/lib64/pkgconfig/openssl.pc
source install_cmake.sh $MODE
