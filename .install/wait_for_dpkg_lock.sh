#!/usr/bin/env bash

# Wait for apt/dpkg lock files to become free.
# Usage:
#   wait_for_dpkg_lock [mode] [timeout_seconds] [poll_seconds]
# Example:
#   wait_for_dpkg_lock sudo 300 5

wait_for_dpkg_lock() {
    local mode="${1:-}"
    local timeout_seconds="${2:-300}"
    local poll_seconds="${3:-5}"
    local start_time elapsed
    local -a lock_files=(
        "/var/lib/dpkg/lock-frontend"
        "/var/lib/dpkg/lock"
        "/var/lib/apt/lists/lock"
        "/var/cache/apt/archives/lock"
    )

    _run_mode_cmd() {
        if [ -n "$mode" ]; then
            # shellcheck disable=SC2086
            $mode "$@"
        else
            "$@"
        fi
    }

    _has_lock_holder() {
        local lock_file="$1"

        if command -v fuser >/dev/null 2>&1; then
            _run_mode_cmd fuser "$lock_file" >/dev/null 2>&1
            return $?
        fi

        if command -v lsof >/dev/null 2>&1; then
            _run_mode_cmd lsof "$lock_file" >/dev/null 2>&1
            return $?
        fi

        # Fallback: no lock-inspection tools available. Let apt run and fail
        # naturally if it still cannot acquire the lock.
        return 1
    }

    _print_lock_diagnostics() {
        local lock_file="$1"
        echo "Diagnostics for lock file: $lock_file"
        _run_mode_cmd ls -l "$lock_file" 2>/dev/null || echo "  lock file missing"

        if command -v fuser >/dev/null 2>&1; then
            echo "  fuser:"
            _run_mode_cmd fuser -v "$lock_file" 2>/dev/null || echo "  no fuser holders"
        fi

        if command -v lsof >/dev/null 2>&1; then
            echo "  lsof:"
            _run_mode_cmd lsof "$lock_file" 2>/dev/null || echo "  no lsof holders"
        fi
    }

    start_time="$(date +%s)"
    while true; do
        local holder_detected=0
        local active_locks=""

        for lock_file in "${lock_files[@]}"; do
            if _has_lock_holder "$lock_file"; then
                holder_detected=1
                active_locks="$active_locks $lock_file"
            fi
        done

        if [ "$holder_detected" -eq 0 ]; then
            return 0
        fi

        elapsed=$(( $(date +%s) - start_time ))
        echo "apt/dpkg lock(s) detected:${active_locks}. waited=${elapsed}s/${timeout_seconds}s"

        if [ "$elapsed" -ge "$timeout_seconds" ]; then
            echo "Timed out waiting for apt/dpkg lock after ${elapsed}s."
            echo "Collecting diagnostics:"

            for lock_file in "${lock_files[@]}"; do
                _print_lock_diagnostics "$lock_file"
            done

            echo "Relevant running processes:"
            _run_mode_cmd ps -eo pid,ppid,etime,user,cmd \
                | grep -E "apt|dpkg|unattended|packagekit" \
                | grep -v "grep -E" || true

            return 1
        fi

        sleep "$poll_seconds"
    done
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    wait_for_dpkg_lock "$@"
fi
