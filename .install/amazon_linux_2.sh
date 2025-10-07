#!/usr/bin/env bash
ARCH=$(uname -m)
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE yum update -y
$MODE yum install -y which

if [[ $ARCH = 'x86_64' ]]
then
    # Install the RPM package that provides the Software Collections (SCL) required for devtoolset-11
    $MODE yum install -y https://vault.centos.org/centos/7/extras/x86_64/Packages/centos-release-scl-rh-2-3.el7.centos.noarch.rpm

    # http://mirror.centos.org/centos/7/ is deprecated, so we changed the above link to `https://vault.centos.org`,
    # and we have to change the baseurl in the repo file to the working mirror (from mirror.centos.org to vault.centos.org)
    $MODE sed -i 's/mirrorlist=/#mirrorlist=/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo                        # Disable mirrorlist
    $MODE sed -i 's/#baseurl=http:\/\/mirror/baseurl=http:\/\/vault/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo # Enable a working baseurl

    $MODE yum install -y wget git devtoolset-11-make rsync unzip libclang-dev clang

    source /opt/rh/devtoolset-11/enable

    cp /opt/rh/devtoolset-11/enable /etc/profile.d/scl-devtoolset-11.sh

    # install newer binutils
    wget https://ftp.gnu.org/gnu/binutils/binutils-2.42.tar.gz
    tar -xzf binutils-2.42.tar.gz
    cd binutils-2.42
    ./configure --prefix=/usr/local
    make -j$(nproc)
    make install
    echo "/usr/local/bin" >> "$GITHUB_PATH"
    as --version   # Should now be 2.42
    cd ..
else
    # Install the RPM package that provides the Software Collections (SCL) required for devtoolset-10
    $MODE yum install -y https://vault.centos.org/altarch/7/extras/aarch64/Packages/centos-release-scl-rh-2-3.el7.centos.noarch.rpm

    # http://mirror.centos.org/centos/7/ is deprecated, so we changed the above link to `https://vault.centos.org`,
    # and we have to change the baseurl in the repo file to the working mirror (from mirror.centos.org to vault.centos.org)
    # Disable mirrorlist
    $MODE sed -i 's/mirrorlist=/#mirrorlist=/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo
    # Enable a working baseurl
    $MODE sed -i 's/#baseurl=http:\/\/mirror.centos.org\/centos/baseurl=http:\/\/vault.centos.org\/altarch/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo

    $MODE yum install -y wget git \
        devtoolset-10-make rsync unzip clang curl libclang-dev

    source /opt/rh/devtoolset-10/enable

    $MODE cp /opt/rh/devtoolset-10/enable /etc/profile.d/scl-devtoolset-10.sh

fi
