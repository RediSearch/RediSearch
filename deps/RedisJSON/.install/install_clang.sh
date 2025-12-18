#!/usr/bin/env sh

rust_llvm_major_version=$(rustc --version --verbose | grep "LLVM version" | awk '{print $3}' | cut -d. -f1)

export CWD=$(dirname `which "${0}"`)
export CLANG_VERSION=${rust_llvm_major_version}
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

wget https://apt.llvm.org/llvm.sh -O llvm.sh

chmod u+x llvm.sh

$MODE ./llvm.sh $CLANG_VERSION all

$MODE apt-get install python3-lldb-${rust_llvm_major_version} --yes

$MODE $CWD/update_clang_alternatives.sh $CLANG_VERSION 1

ls /bin/

$MODE clang --version
$MODE clang++ --version
$MODE llvm-cov --version
$MODE llvm-profdata --version

