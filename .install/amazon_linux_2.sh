#!/bin/bash
ARCH=$(uname -m)
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE yum update -y

if [[ $ARCH = 'x86_64' ]]
then
    # Install the RPM package that provides the Software Collections (SCL) required for devtoolset-11
    $MODE yum install -y https://vault.centos.org/centos/7/extras/x86_64/Packages/centos-release-scl-rh-2-3.el7.centos.noarch.rpm

    # http://mirror.centos.org/centos/7/ is deprecated, so we changed the above link to `https://vault.centos.org`,
    # and we have to change the baseurl in the repo file to the working mirror (from mirror.centos.org to vault.centos.org)
    $MODE sed -i 's/mirrorlist=/#mirrorlist=/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo                        # Disable mirrorlist
    $MODE sed -i 's/#baseurl=http:\/\/mirror/baseurl=http:\/\/vault/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo # Enable a working baseurl

    $MODE yum install -y wget git which devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-make rsync python3 unzip

    source /opt/rh/devtoolset-11/enable

    cp /opt/rh/devtoolset-11/enable /etc/profile.d/scl-devtoolset-11.sh
else
    # Install the RPM package that provides the Software Collections (SCL) required for devtoolset-10
    $MODE yum install -y https://vault.centos.org/altarch/7/extras/aarch64/Packages/centos-release-scl-rh-2-3.el7.centos.noarch.rpm

    # http://mirror.centos.org/centos/7/ is deprecated, so we changed the above link to `https://vault.centos.org`,
    # and we have to change the baseurl in the repo file to the working mirror (from mirror.centos.org to vault.centos.org)
    # Disable mirrorlist
    $MODE sed -i 's/mirrorlist=/#mirrorlist=/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo
    # Enable a working baseurl
    $MODE sed -i 's/#baseurl=http:\/\/mirror.centos.org\/centos/baseurl=http:\/\/vault.centos.org\/altarch/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo

    $MODE yum install -y wget git which devtoolset-10-gcc devtoolset-10-gcc-c++ \
        devtoolset-10-make rsync python3 python3-devel unzip clang

    source /opt/rh/devtoolset-10/enable

    $MODE cp /opt/rh/devtoolset-10/enable /etc/profile.d/scl-devtoolset-10.sh
    
    # hack gcc 10.2.1 Redhat to enable _GLIBCXX_USE_CXX11_ABI=1
    $MODE sed -i \
        -e 's/^# define _GLIBCXX_USE_DUAL_ABI 0/# define _GLIBCXX_USE_DUAL_ABI 1/g' \
        -e 's/^# define _GLIBCXX_USE_CXX11_ABI 0/# define _GLIBCXX_USE_CXX11_ABI 1/g' \
        /opt/rh/devtoolset-10/root/usr/include/c++/10/aarch64-redhat-linux/bits/c++config.h
fi

$MODE yum install -y openssl11 openssl11-devel
$MODE ln -s `which openssl11` /usr/bin/openssl
source install_cmake.sh $MODE
