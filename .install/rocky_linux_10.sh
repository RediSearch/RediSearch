#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
$MODE dnf update -y

# Rocky 10 ships GCC 14 natively — no need for gcc-toolset
$MODE dnf install -y gcc gcc-c++ make wget git diffutils --nobest --skip-broken --allowerasing

# python3-devel: the test requirements build C extensions (psutil) against the system Python
$MODE dnf install -y openssl openssl-devel python3-devel which rsync unzip curl gdb xz --nobest --skip-broken --allowerasing

# AppStream's clang on Rocky 10 is 22, and this branch's bindgen generates wrong struct
# layouts against libclang 22. Use EPEL's side-by-side LLVM 21 and put it first, so
# bindgen (through llvm-config) and an unversioned clang both resolve to it.
$MODE dnf install -y epel-release
$MODE dnf install -y clang21 clang21-devel
cat <<'PROFILE' | $MODE tee /etc/profile.d/llvm21.sh >/dev/null
export PATH="/usr/lib64/llvm21/bin:$PATH"
export LIBCLANG_PATH=/usr/lib64/llvm21/lib64
PROFILE

# Rocky 9 got this from gcc-toolset; with the system GCC it is a separate package. This branch's
# Boost multiprecision includes quadmath.h where __float128 exists (x86_64 only).
if [[ $(uname -m) == x86_64 ]]; then
    $MODE dnf install -y libquadmath-devel
fi
