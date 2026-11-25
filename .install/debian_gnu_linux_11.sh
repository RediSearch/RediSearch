#!/usr/bin/env bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

# snapshot.debian.org intermittently fails downloads with 503/504, which APT does not retry.
# Bullseye's APT reports a failed index fetch only as a warning and `apt update` still exits 0,
# so retry update and install as one step. A rerun fetches only what is still missing.
apt_retry() {
    for attempt in 1 2 3 4 5; do
        $MODE apt update -qq && $MODE apt install -yqq "$@" && return 0
        echo "apt update/install failed (attempt $attempt)" >&2
        [ "$attempt" -lt 5 ] && sleep $((attempt * 15))
    done
    return 1
}

# Cached images install dependencies before the workflow bootstrap can configure APT.
$MODE tee /etc/apt/sources.list > /dev/null <<'EOF'
deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260901T000000Z/ bullseye main
deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/20260901T000000Z/ bullseye-security main
deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260901T000000Z/ bullseye-updates main
EOF

apt_retry git wget build-essential lcov openssl libssl-dev \
        rsync unzip curl libclang-dev gdb
