#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/..; pwd)
cd $ROOT

SAN_PREFIX=/opt/llvm-project/build-msan

build() {
	flags=""

	if [[ $ASAN == 1 ]]; then
		mode=asan
		flags="-DUSE_ASAN=ON"
	elif [[ $MSAN == 1 ]]; then
		mode=msan
		flags="-DUSE_MSAN=ON -DMSAN_PREFIX=${SAN_PREFIX}"
	else
		echo "Should define either ASAN=1 or MSAN=1"
		exit 1
	fi

	rm -rf build-${mode}
	mkdir -p build-${mode}
	cd build-${mode}

	cmake -DCMAKE_BUILD_TYPE=DEBUG \
		-DCMAKE_C_COMPILER=clang \
		-DCMAKE_CXX_COMPILER=clang++ \
		-DRS_RUN_TESTS=ON \
		$flags \
		..

	if [[ -z $CI_CONCURRENCY ]]; then
		CI_CONCURRENCY=$($ROOT/deps/readies/bin/nproc)
	fi
	if [[ $CI_CONCURRENCY > 20 ]]; then
		CI_CONCURRENCY=20
	fi

	make -j$CI_CONCURRENCY
}

ASAN=1 build
MSAN=1 build
