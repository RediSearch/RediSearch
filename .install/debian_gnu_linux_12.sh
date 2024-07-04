#!/bin/bash
ARCH=$(uname -m)
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
        python3 python3-venv python3-dev rsync unzip

# Install GCC 13 on x86_64 because GCC 12 fails compilating VectorSimilarity
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=105593
if [[ $ARCH = 'x86_64' ]]
then
    # Download and compile GCC 13
    cd /tmp
    wget https://ftp.gnu.org/gnu/gcc/gcc-13.3.0/gcc-13.3.0.tar.gz
    tar -xf gcc-13.3.0.tar.gz
    cd gcc-13.3.0
    $MODE ./contrib/download_prerequisites
    ./configure --disable-multilib --enable-languages=c,c++
    make -j$(nproc)
    $MODE make install
    # Update alternatives
    $MODE update-alternatives --install /usr/bin/gcc gcc /usr/local/bin/gcc 60 \
        --slave /usr/bin/g++ g++ /usr/local/bin/g++
    cd $OLDPWD
fi

source install_cmake.sh $MODE
