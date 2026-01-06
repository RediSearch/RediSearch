#!/usr/bin/env bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
    unzip rsync clang curl libclang-dev

# We need Python headers to build psutil@5.x.y from
# source, since it only started providing wheels for aarch64
# in version 6.w.z.
if [ "$(uname -m)" = "aarch64" ]; then
    $MODE apt install -y python3-dev
fi

# Need to same LLVM version than the one used by rustc
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 21 all

# Set alternatives so this version is used by default
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-21 110
sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-21 110
sudo update-alternatives --install /usr/bin/lld lld /usr/bin/ld.lld-21 110
