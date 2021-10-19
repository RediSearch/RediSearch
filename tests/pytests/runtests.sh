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
		MODARGS=args          RediSearch module arguments
		TEST=name             Operate in single-test mode
		ONLY_STABLE=1         Skip unstable tests

		REDIS_SERVER=path     Redis Server command
		REDIS_VERBOSE=1       (legacy) Verbose ouput
		CONFIG_FILE=file      Path to config file
		EXISTING_ENV=1        Run the tests on existing env

		VG=1                  Use valgrind
		VG_LEAKS=0            Do not detect leaks
		SAN=type              Use LLVM sanitizer (type=address|memory|leak|thread) 
		GDB=1                 Enable interactive gdb debugging (in single-test mode)

		VERBOSE=1             Print commands and Redis output
		IGNERR=1              Do not abort on error
		NOP=1                 Dry run


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

MODULE="$1"
shift

OP=
[[ $NOP == 1 ]] && OP=echo

if [[ -n $TEST ]]; then
	[[ $GDB == 1 ]] && RLTEST_ARGS+=" -i"
	RLTEST_ARGS+=" -v -s --test $TEST"
	export RUST_BACKTRACE=1
fi

#---------------------------------------------------------------------------------------------- 

if [[ -n $SAN ]]; then
	if ! grep THPIsEnabled /build/redis.blacklist &> /dev/null; then
		echo "fun:THPIsEnabled" >> /build/redis.blacklist
	fi
	export RS_GLOBAL_DTORS=1

	export SANITIZER="$SAN"

	if [[ $SAN == addr || $SAN == address ]]; then
		REDIS_SERVER=${REDIS_SERVER:-redis-server-asan-6.2}
		if ! command -v $REDIS_SERVER > /dev/null; then
			echo Building Redis for clang-asan ...
			$READIES/bin/getredis --force -v 6.2 --own-openssl --no-run --suffix asan --clang-asan --clang-san-blacklist /build/redis.blacklist
		fi

		export ASAN_OPTIONS=detect_odr_violation=0:detect_leaks=0

	elif [[ $SAN == memory ]]; then
		REDIS_SERVER=${REDIS_SERVER:-redis-server-msan-6.2}
		if ! command -v $REDIS_SERVER > /dev/null; then
			echo Building Redis for clang-msan ...
			$READIES/bin/getredis --force -v 6.2  --no-run --own-openssl --suffix msan --clang-msan --llvm-dir /opt/llvm-project/build-msan --clang-san-blacklist /build/redis.blacklist
		fi
	fi

elif [[ $VG == 1 ]]; then
	REDIS_SERVER=${REDIS_SERVER:-redis-server-vg}
	if ! command -v $REDIS_SERVER > /dev/null; then
		echo Building Redis for Valgrind ...
		$READIES/bin/getredis -v 6 --valgrind --suffix vg
	fi
	VALGRIND_ARGS=--use-valgrind
	if [[ $VG_LEAKS == 0 ]]; then
		export VG_OPTIONS="--leak-check=no --track-origins=yes --suppressions=$ROOT/tests/valgrind/redis_valgrind.sup"
		VALGRIND_ARGS+=" --vg-no-leakcheck --vg-options=\"--leak-check=no --track-origins=yes --suppressions=$ROOT/tests/valgrind/redis_valgrind.sup\" "
	fi

	export VALGRIND=1
	export RS_GLOBAL_DTORS=1

else
	REDIS_SERVER=${REDIS_SERVER:-redis-server}
fi

if ! command -v $REDIS_SERVER > /dev/null; then
	echo "Cannot find $REDIS_SERVER. Aborting."
	exit 1
fi

#---------------------------------------------------------------------------------------------- 

# ARGS="--clear-logs"
# ARGS="--unix"

if [[ $REDIS_VERBOSE == 1 || $VERBOSE ]]; then
    RLTEST_ARGS+=" -s -v"
fi

#---------------------------------------------------------------------------------------------- 

if [[ $EXISTING_ENV == 1 ]]; then
	xredis_conf=$(mktemp "${TMPDIR:-/tmp}/xredis_conf.XXXXXXX")
	rm -f $xredis_conf
	cat <<-EOF > $xredis_conf
		loadmodule $MODULE $MODARGS
		EOF

	rltest_config=$(mktemp "${TMPDIR:-/tmp}/xredis_rltest.XXXXXXX")
	rm -f $rltest_config
	cat <<-EOF > $rltest_config
		--env existing-env
		$RLTEST_ARGS
		$@

		EOF

	if [[ $VERBOSE == 1 ]]; then
		echo "External redis-server configuration:"
		cat $xredis_conf
	fi

	$REDIS_SERVER $xredis_conf &
	XREDIS_PID=$!
	echo "external redis-server pid: " $XREDIS_PID

else
	rltest_config=$(mktemp "${TMPDIR:-/tmp}/rltest.XXXXXXX")
	rm -f $rltest_config
	cat <<-EOF > $rltest_config
		--oss-redis-path=$REDIS_SERVER
		--module $MODULE
		--module-args '$MODARGS'
		$RLTEST_ARGS
		$VALGRIND_ARGS
		$@

		EOF
	if [[ $VERBOSE == 1 ]]; then
		echo "# RLTest config:"
		cat $rltest_config
	fi
fi

#---------------------------------------------------------------------------------------------- 

# Use configuration file in the current directory if it exists
if [[ -n $CONFIG_FILE && -e $CONFIG_FILE ]]; then
	cat $CONFIG_FILE >> $rltest_config
fi

if [[ $VERBOSE == 1 ]]; then
	echo "RLTest configuration:"
	cat $rltest_config
fi

E=0
if [[ $NOP != 1 ]]; then
	{ $OP python2 -m RLTest @$rltest_config; (( E |= $? )); } || true
else
	$OP python2 -m RLTest @$rltest_config
fi
rm -f $rltest_config

if [[ -n $XREDIS_PID ]]; then
	echo "killing external redis-server: $XREDIS_PID"
	kill -9 $XREDIS_PID
fi

exit $E
