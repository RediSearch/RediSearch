#!/bin/bash

# The purpose of this script is to aid in the locating of the RLtest package.
# Normally, this package should be found within the current python site
# installation via `pip`. However, often times, it may be necessary to tweak
# and/or otherwise modify the RLTest package for better visibility or quick
# fixes, and this would be more cumbersome if the cycle required installation
# via pip.
# This script allows the user to specify an alternative directory for rltest,
# and also hides the details of how this module is run (`python -m RLTest`).

# Uncomment following line to see what shell script is doing
set -x

SCRIPT_DIR=$( cd ${0%/*} && pwd -P )

# If current directory has RLTest in it, then we're good to go
if [ -d "${SCRIPT_DIR}/RLTest" ]; then
    true
    # Do nothing - tests are already loaded
fi


if [ -n "$RLTEST" ]; then
    # Specifically search for it in the specified location
    PYTHONPATH="$PYTHONPATH:$RLTEST"
else
    # Assume there is a sibling directory called `RLTest`
    PYTHONPATH="$PYTHONPATH:$SCRIPT_DIR"
fi

export PYTHONPATH


# See if there is a configuration file in the current directory. Use it
# if it exists

# ARGS="--clear-logs"
# ARGS="--unix"

if [ -e rltest.config ]; then
    ARGS="@rltest.config ${ARGS}"
fi

if [ -n "$REDIS_VERBOSE" ]; then
    ARGS="${ARGS} --no-output-catch"
fi



exec python -m RLTest $ARGS "$@"
