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
		VG=1|0                Use valgrind
		SAN=type              Use LLVM sanitizer (type=address|memory|leak|thread) 
		GDB=0|1               Enable interactive gdb debugging (in single-test mode)
		REJSON=0|1|get        Also load RedisJSON module (get: force download from S3)
		REJSON_BRANCH=branch  Use a snapshot of given branch name
		REJSON_PATH=path      RedisJSON module path
		REJSON_MODARGS=args   RedisJSON module arguments
		REDIS_SERVER=path     Redis Server command
		REDIS_VERBOSE=0|1     (legacy) Verbose ouput
		CONFIG_FILE=file      Path to config file

		VERBOSE=1          Print commands and Redis output
		IGNERR=1           Do not abort on error
		NOP=1              Dry run
		EXISTING_ENV=1     Run the tests on existing env


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
	export ASAN_OPTIONS=detect_odr_violation=0
	export RS_GLOBAL_DTORS=1
	
	rejson_path=$ROOT/deps/RedisJSON/target/x86_64-unknown-linux-gnu/debug/rejson.so
	if [[ -z $REJSON_PATH && -f $rejson_path ]]; then
		export REJSON_PATH=$rejson_path
	fi
fi

if [[ $VG == 1 ]]; then
	REDIS_SERVER=${REDIS_SERVER:-redis-server-vg}
	if ! command -v $REDIS_SERVER > /dev/null; then
		echo Building Redis for Valgrind ...
		$READIES/bin/getredis -v 6 --valgrind --suffix vg
	fi
elif [[ $SAN == addr || $SAN == address ]]; then
	REDIS_SERVER=${REDIS_SERVER:-redis-server-asan}	
	if ! command -v $REDIS_SERVER > /dev/null; then
		echo Building Redis for Valgrind ...
		$READIES/bin/getredis --force -v 6.0 --no-run --suffix asan --clang-asan --clang-san-blacklist /build/redis.blacklist
	fi
elif [[ $SAN == memory ]]; then
	REDIS_SERVER=${REDIS_SERVER:-redis-server-msan}
	if ! command -v $REDIS_SERVER > /dev/null; then
		echo Building Redis for Valgrind ...
		$READIES/bin/getredis --force -v 6.0  --no-run --suffix msan --clang-msan --llvm-dir /opt/llvm-project/build-msan --clang-san-blacklist /build/redis.blacklist
	fi
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

REJSON_BRANCH=${REJSON_BRANCH:-master}

if [[ -n $REJSON && $REJSON != 0 ]]; then
	platform=`$READIES/bin/platform -t`
	if [[ -n $REJSON_PATH ]]; then
		REJSON_ARGS="--module $REJSON_PATH"
		REJSON_MODULE="$REJSON_PATH"
	else
		REJSON_MODULE="$ROOT/bin/$platform/RedisJSON/rejson.so"
		if [[ ! -f $REJSON_MODULE || $REJSON == get ]]; then
			FORCE_GET=
			[[ $REJSON == get ]] && FORCE_GET=1
			BRANCH=$REJSON_BRANCH FORCE=$FORCE_GET $OP $ROOT/sbin/get-redisjson
		fi
		REJSON_ARGS="--module $REJSON_MODULE"
	fi
	REJSON_ARGS+=" --module-args '$REJSON_MODARGS'"
fi

#---------------------------------------------------------------------------------------------- 

if [[ $EXISTING_ENV == 1 ]]; then
	RLTEST_ARGS+=" --env existing-env"
	# also start the redis server on which the tests will run
	EXISTING_ENV_ARGS="--loadmodule $MODULE $MODARGS"
	if [[ $REJSON_MODULE ]]; then
		EXISTING_ENV_ARGS="$EXISTING_ENV_ARGS --loadmodule $REJSON_MODULE $REJSON_MODARGS"
	fi
	redis-server $EXISTING_ENV_ARGS &
	EXTERNAL_REDIS_PID=$!
	echo "external process pid: " $EXTERNAL_REDIS_PID
fi

#---------------------------------------------------------------------------------------------- 

if [[ $VG == 1 ]]; then
	VALGRIND_ARGS=--use-valgrind
fi

#---------------------------------------------------------------------------------------------- 

config=$(mktemp "${TMPDIR:-/tmp}/rltest.XXXXXXX")
rm -f $config
cat << EOF > $config

--oss-redis-path=$REDIS_SERVER
--module $MODULE
--module-args '$MODARGS'
$RLTEST_ARGS
$REJSON_ARGS
$VALGRIND_ARGS
$@

EOF

# Use configuration file in the current directory if it exists
if [[ -n $CONFIG_FILE && -e $CONFIG_FILE ]]; then
	cat $CONFIG_FILE >> $config
fi

if [[ $VERBOSE == 1 ]]; then
	echo "RLTest configuration:"
	cat $config
fi

export OS=$($READIES/bin/platform --os)

$OP python2 -m RLTest @$config
EXIT_CODE=$?
rm -f $config

if [[ $EXTERNAL_REDIS_PID ]]; then
	echo "killing external process: " $EXTERNAL_REDIS_PID
	kill -s 9 $EXTERNAL_REDIS_PID
fi

exit $EXIT_CODE