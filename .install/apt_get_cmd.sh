#!/usr/bin/env bash
# Shared apt-get wrapper with dpkg lock timeout for CI environments.
#
# Self-hosted GitHub Actions runners may have background apt-daily or
# unattended-upgrades services that grab the dpkg lock.  The timeout
# lets apt-get wait instead of failing immediately.
#
# Usage: source this file, then call:
#   apt_get_cmd <mode> <apt-get-args...>
# where <mode> is a privilege-escalation prefix ("sudo" or "").

APT_GET_LOCK_TIMEOUT_SECONDS="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}"

apt_get_cmd() {
    local mode="$1"; shift
    $mode apt-get -o DPkg::Lock::Timeout="$APT_GET_LOCK_TIMEOUT_SECONDS" "$@"
}
