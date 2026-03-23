#!/bin/bash
# Retry wrapper script - runs a command with retries on failure
# Usage: retry.sh <command> [args...]
#
# This script will retry the given command up to 5 times with a 30 second
# delay between attempts. Mirrors the SETUP_RETRY_LOOP pattern from
# .github/workflows/task-test.yml

set -eo pipefail

SUCCESS=0
for i in 1 2 3 4 5; do
  echo "Attempt $i of 5"
  if "$@"; then
    echo "Setup succeeded"
    SUCCESS=1
    break
  fi
  if [ $i -lt 5 ]; then
    echo "Setup failed, retrying in 30 seconds..."
    sleep 30
  fi
done

[ $SUCCESS -eq 1 ] || exit 1

