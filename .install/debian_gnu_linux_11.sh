#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

# Cached images install dependencies before the workflow bootstrap can configure APT.
# Keep these sources in sync with bullseye_setup in task-get-config.yml.
$MODE tee /etc/apt/sources.list > /dev/null <<'EOF'
deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260901T000000Z/ bullseye main
deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/20260901T000000Z/ bullseye-security main
deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260901T000000Z/ bullseye-updates main
EOF

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
        rsync unzip curl libclang-dev gdb
