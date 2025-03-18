#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
ARCH=$([[ $(uname -m) == x86_64 ]] && echo x86_64 || echo noarch)

# http://mirror.centos.org/centos/7/ is deprecated, so we have to disable mirrorlists
# and change the baseurl in the repo file to the working mirror (from mirror.centos.org to vault.centos.org)
set_all_baseurls() {
    for file in /etc/yum.repos.d/*.repo; do
        $MODE sed -i 's/^mirrorlist=/#mirrorlist=/g' $file
        $MODE sed -i 's/^#[[:space:]]*baseurl=http:\/\/mirror/baseurl=http:\/\/vault/g' $file
    done
}

set_all_baseurls # set the baseurls to the working mirror before installing basic packages
$MODE yum update -y
$MODE yum install -y https://packages.endpointdev.com/rhel/7/os/${ARCH}/endpoint-repo.${ARCH}.rpm
$MODE yum groupinstall -y "Development Tools"
$MODE yum -y install centos-release-scl

set_all_baseurls # set the baseurls again before installing devtoolset-11 (some new repos were added)
$MODE yum -y install openssl-devel openssl bzip2-devel libffi-devel wget which git sqlite sqlite-devel\
    devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-make devtoolset-11-libatomic-devel rsync

source /opt/rh/devtoolset-11/enable

cp /opt/rh/devtoolset-11/enable /etc/profile.d/scl-devtoolset-11.sh

# get a newer libstc++ library. The one that comes with the gcc version (6.0.19) will fail with
# the error: `/usr/lib64/libstdc++.so.6: version `GLIBCXX_3.4.20' not found`
wget https://redismodules.s3.amazonaws.com/gnu/libstdc%2B%2B.so.6.0.25-linux-x64.tgz
file_name=$(tar -xvf libstdc++.so.6.0.25-linux-x64.tgz)
$MODE mv $file_name /usr/lib64/
cd /usr/lib64
$MODE rm -f libstdc++.so.6  # remove the old symlink
$MODE ln -s libstdc++.so.6.0.25 libstdc++.so.6
cd -

wget https://www.python.org/ftp/python/3.9.6/Python-3.9.6.tgz
tar -xvf Python-3.9.6.tgz
cd Python-3.9.6
./configure --enable-optimizations
$MODE make altinstall
update-alternatives --install /usr/bin/python3 python3 `which python3.9` 2
