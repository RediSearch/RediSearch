#!/usr/bin/env bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APT_LOCK_WAIT_TIMEOUT_SECONDS="${APT_LOCK_WAIT_TIMEOUT_SECONDS:-60}"

source "${SCRIPT_DIR}/wait_for_dpkg_lock.sh"

# On self-hosted CI runners (especially the intel path), background package
# jobs (e.g. unattended-upgrades) can hold dpkg/apt locks for a while.
# Waiting here reduces flaky failures like:
# "Unable to acquire the dpkg frontend lock".
wait_for_dpkg_lock "$MODE" "$APT_LOCK_WAIT_TIMEOUT_SECONDS"

$MODE apt update -qq
$MODE apt install -yqq gcc-12 g++-12 git wget build-essential lcov openssl libssl-dev unzip rsync curl libclang-dev gdb
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 60 --slave /usr/bin/g++ g++ /usr/bin/g++-12
# align gcov version with gcc version
$MODE update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-12 60
