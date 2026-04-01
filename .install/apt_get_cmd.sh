#!/usr/bin/env bash
# Shared apt-get wrapper with dpkg lock timeout for CI environments.
#
# Self-hosted GitHub Actions runners may have background apt-daily or
# unattended-upgrades services that grab the dpkg lock.  The timeout
# lets apt-get wait instead of failing immediately.
#
# Usage: source this file from any Debian/Ubuntu install script.
#   MODE must be set before sourcing (empty string or "sudo").

APT_GET_LOCK_TIMEOUT_SECONDS="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}"

apt_get_cmd() {
    $MODE apt-get -o DPkg::Lock::Timeout="$APT_GET_LOCK_TIMEOUT_SECONDS" "$@"
}
