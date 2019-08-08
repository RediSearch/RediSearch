#!/bin/bash
set -e
set -x

./.circleci/ci_get_deps.sh

SAN_PREFIX=/opt/san

mode=$1
extra_flags=""

if [ "$mode" == "asan" ]; then
    extra_flags="-DUSE_ASAN=ON"
elif [ "$mode" == "msan" ]; then
    extra_flags="-DUSE_MSAN=ON -DMSAN_PREFIX=${SAN_PREFIX}"
else
    echo "Mode must be 'asan' or 'msan'"
    exit 1
fi

mkdir build-${mode}
cd build-${mode}

cmake -DCMAKE_BUILD_TYPE=DEBUG \
    -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ \
    -DRS_RUN_TESTS=ON \
    $extra_flags \
    ..

make -j20 # should build ok

## Add some configuration options to our rltest file

cat >rltest.config <<EOF
--oss-redis-path=${SAN_PREFIX}/bin/redis-server-${mode}
--no-output-catch
--exit-on-failure
--check-exitcode
--unix
EOF

export ASAN_OPTIONS=detect_odr_violation=0

# FIXME: Need to change the image once this actually works..
ln -s /usr/bin/llvm-symbolizer-4.0 /usr/bin/llvm-symbolizer || true

ctest --output-on-failure -j20
