#!/bin/bash

[[ $IGNERR == 1 ]] || set -e
# [[ $VERBOSE == 1 ]] && set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/../.. && pwd)
READIES=$ROOT/deps/readies

cd $HERE

#----------------------------------------------------------------------------------------------

if [[ $1 == --help || $1 == help ]]; then
	cat <<-END
		Run Python tests using RLTest

		[ARGVARS...] runtests.sh [--help|help] [<module-so-path>] [extra RLTest args...]

		Argument variables:
		TEST=name             Operate in single-test mode
		GDB=0|1               Enable interactive gdb debugging (in single-test mode)
		REJSON=0|1|get        Also load RedisJSON module (get: force download from S3)
		REJSON_PATH=path      RedisJSON module path (implies REJSON=1)
		REJSON_MODARGS=args   RedisJSON module arguments
		REDIS_VERBOSE=0|1     (legacy) Verbose ouput

		VERBOSE=1          Print commands and Redis output
		IGNERR=1           Do not abort on error
		NOP=1              Dry run


	END
	exit 0
fi

#---------------------------------------------------------------------------------------------- 

# If current directory has RLTest in it, then we're good to go
if [[ -d $HERE/RLTest ]]; then
    true
    # Do nothing - tests are already loaded
fi

if [[ -n $RLTEST ]]; then
    # Specifically search for it in the specified location
    PYTHONPATH="$PYTHONPATH:$RLTEST"
else
    # Assume there is a sibling directory called `RLTest`
    PYTHONPATH="$PYTHONPATH:$HERE"
fi

export PYTHONPATH

#---------------------------------------------------------------------------------------------- 

if ! command -v redis-server > /dev/null; then
	echo "Cannot find redis-server. Aborting."
	exit 1
fi

#---------------------------------------------------------------------------------------------- 

MODULE="$1"
shift

OP=
[[ $NOP == 1 ]] && OP=echo

if [[ -n $TEST ]]; then
	[[ $GDB == 1 ]] && RLTEST_ARGS+=" -i"
	RLTEST_ARGS+=" -v -s --test $TEST"
fi

#---------------------------------------------------------------------------------------------- 

# ARGS="--clear-logs"
# ARGS="--unix"

if [[ $REDIS_VERBOSE == 1 || $VERBOSE ]]; then
    RLTEST_ARGS+=" -s -v"
fi

#---------------------------------------------------------------------------------------------- 

[[ -n $REJSON_PATH ]] && REJSON=1
if [[ -n $REJSON && $REJSON != 0 ]]; then
	platform=`$READIES/bin/platform -t`
	if [[ -n $REJSON_PATH ]]; then
		REJSON_ARGS="--module $REJSON_PATH"
		REJSON_MODULE="$REJSON_PATH"
	else
		REJSON_MODULE="$ROOT/bin/$platform/RedisJSON/rejson.so"
		[[ ! -f $REJSON_MODULE || $REJSON == get ]] && $OP $ROOT/sbin/get-rejson
		REJSON_ARGS="--module $REJSON_MODULE"
	fi
	REJSON_ARGS+=" --module-args '$REJSON_MODARGS'"
fi

#---------------------------------------------------------------------------------------------- 

config=$(mktemp "${TMPDIR:-/tmp}/rltest.XXXXXXX")
rm -f $config
cat << EOF > $config

--module $MODULE
--module-args '$MODARGS'
$RLTEST_ARGS
$REJSON_ARGS
$@

EOF

# Use configuration file in the current directory if it exists
if [[ -e rltest.config ]]; then
	cat rltest.config >> $config
fi

if [[ $VERBOSE == 1 ]]; then
	echo "RLTest configuration:"
	cat $config
fi

$OP exec python -m RLTest @$config
rm -f $config
