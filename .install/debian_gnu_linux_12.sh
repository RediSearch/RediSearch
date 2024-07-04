#!/bin/bash
ARCH=$(uname -m)
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
        python3 python3-venv python3-dev rsync unzip

# Patch VectorSimilarity because GCC 12 fails to compile it
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=105593
if [[ $ARCH = 'x86_64' ]]
then
        sed -i 's/set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall")/set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wno-error=uninitialized")/g' \
                ../deps/VectorSimilarity/src/VecSim/spaces/CMakeLists.txt
fi

source install_cmake.sh $MODE
