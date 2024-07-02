#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE yum update -y
# Install the RPM package that provides the Software Collections (SCL) required for devtoolset-11
$MODE yum install -y https://vault.centos.org/centos/7/extras/x86_64/Packages/centos-release-scl-rh-2-3.el7.centos.noarch.rpm

$MODE sed -i 's/mirrorlist=/#mirrorlist=/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo
$MODE sed -i 's/#baseurl=http:\/\/mirror.centos.org/baseurl=http:\/\/mirror.nsc.liu.se\/centos-store/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo

$MODE yum install -y wget git which devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-make rsync python3 unzip

source /opt/rh/devtoolset-11/enable

cp /opt/rh/devtoolset-11/enable /etc/profile.d/scl-devtoolset-11.sh

$MODE yum install -y openssl11 openssl11-devel
$MODE ln -s `which openssl11` /usr/bin/openssl
source install_cmake.sh $MODE
