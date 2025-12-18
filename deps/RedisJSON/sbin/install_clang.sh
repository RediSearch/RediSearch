#!/usr/bin/env sh

export CWD=$(dirname `which "${0}"`)
export CLANG_VERSION=18
export DEBIAN_FRONTEND=noninteractive

wget https://apt.llvm.org/llvm.sh -O llvm.sh

chmod u+x llvm.sh

# expected to fail:
./llvm.sh $CLANG_VERSION

apt-get install python3-lldb-18 --yes --force-yes

./llvm.sh $CLANG_VERSION

$CWD/update_clang_alternatives.sh $CLANG_VERSION 1
