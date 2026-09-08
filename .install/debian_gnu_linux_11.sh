#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

# The minimal image needs CA certificates before it can use HTTPS snapshots.
# Keep these sources in sync with bullseye_setup in task-get-linux-configurations.yml.
$MODE tee /etc/apt/sources.list > /dev/null <<'EOF'
deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260901T000000Z/ bullseye main
deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20260901T000000Z/ bullseye-security main
deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260901T000000Z/ bullseye-updates main
EOF

$MODE apt update -qq
$MODE apt install -yqq ca-certificates
$MODE sed -i 's|http://snapshot.debian.org/|https://snapshot.debian.org/|g' /etc/apt/sources.list

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
        rsync unzip curl libclang-dev gdb
