
#!/bin/bash
ARCH=$(uname -m)
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
        rsync unzip curl libclang-dev
