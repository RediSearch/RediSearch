#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE yum update -y
#$MODE yum groupinstall -y "Development Tools"
#$MODE yum remove -y gcc # remove gcc 7
$MODE yum install -y http://mirror.centos.org/centos/7/extras/x86_64/Packages/centos-release-scl-rh-2-3.el7.centos.noarch.rpm

$MODE yum install -y wget git which devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-make rsync python3 unzip #gnutls

source /opt/rh/devtoolset-11/enable

cp /opt/rh/devtoolset-11/enable /etc/profile.d/scl-devtoolset-11.sh
bash

echo "gcc version: $(gcc --version)"
# Install 'openss11' and make it the default so we will use it when linking.
# Currently it is commented out and handled on 'sbin/setup'. This is becauses
# So if we install 'openssl11-devel' here we will cause 'sbin/setup' to fail.
# Once we remove the system setup we can uncomment those lines and install 'openssl11' and 'openssl11-devel'.
# When we do this we should also remember to remove the 'openssl' and 'openssl-devel' two lines above.
$MODE yum remove -y openssl-devel
$MODE yum install -y openssl11 openssl11-devel
ln -s `which openssl11` /usr/bin/openssl
ln -s /usr/lib64/pkgconfig/libssl11.pc /usr/lib64/pkgconfig/libssl.pc
ln -s /usr/lib64/pkgconfig/libcrypto11.pc /usr/lib64/pkgconfig/libcrypto.pc
ln -s /usr/lib64/pkgconfig/openssl11.pc /usr/lib64/pkgconfig/openssl.pc
source install_cmake.sh $MODE
