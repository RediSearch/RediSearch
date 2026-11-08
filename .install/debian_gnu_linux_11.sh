#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

# snapshot.debian.org intermittently fails downloads with 503/504, which APT does not retry.
# A rerun fetches only what is still missing.
apt_retry() {
    for attempt in 1 2 3 4 5; do
        $MODE apt "$@" && return 0
        echo "apt $1 failed (attempt $attempt)" >&2
        [ "$attempt" -lt 5 ] && sleep $((attempt * 15))
    done
    return 1
}

# The minimal image needs CA certificates before it can use HTTPS snapshots.
# Keep these sources in sync with bullseye_setup in task-get-linux-configurations.yml.
$MODE tee /etc/apt/sources.list > /dev/null <<'EOF'
deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260901T000000Z/ bullseye main
deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20260901T000000Z/ bullseye-security main
deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260901T000000Z/ bullseye-updates main
EOF

apt_retry update -qq
apt_retry install -yqq ca-certificates
$MODE sed -i 's|http://snapshot.debian.org/|https://snapshot.debian.org/|g' /etc/apt/sources.list

apt_retry update -qq
apt_retry install -yqq git wget build-essential lcov openssl libssl-dev \
        rsync unzip curl libclang-dev gdb
