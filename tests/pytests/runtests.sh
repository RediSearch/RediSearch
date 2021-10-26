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
		RLTEST_ARGS=args      Extra RLTest args
		MODARGS=args          RediSearch module arguments
		COORD=oss|rlec        Test Coordinator
		TEST=name             Operate in single-test mode
		ONLY_STABLE=1         Skip unstable tests

		REJSON=0|1|get        Also load RedisJSON module (get: force download from S3)
		REJSON_BRANCH=branch  Use a snapshot of given branch name
		REJSON_PATH=path      RedisJSON module path
		REJSON_MODARGS=args   RedisJSON module arguments

		REDIS_SERVER=path     Redis Server command
		REDIS_VERBOSE=1       (legacy) Verbose ouput
		CONFIG_FILE=file      Path to config file
		EXISTING_ENV=1        Run the tests on existing env

		COV=1				  Run with coverage analysis
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

RLTEST_ARGS+=" $@"
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
	export SHORT_READ_BYTES_DELTA=512
	
	rejson_path=$ROOT/deps/RedisJSON/target/x86_64-unknown-linux-gnu/debug/rejson.so
	if [[ -z $REJSON_PATH && -f $rejson_path ]]; then
		export REJSON_PATH=$rejson_path
	fi

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
	export SHORT_READ_BYTES_DELTA=512

else
	REDIS_SERVER=${REDIS_SERVER:-redis-server}
fi

if ! command -v $REDIS_SERVER > /dev/null; then
	echo "Cannot find $REDIS_SERVER. Aborting."
	exit 1
fi

#---------------------------------------------------------------------------------------------- 

if [[ $COV == 1 ]]; then
	COV_ARGS=--unix
	
	export CODE_COVERAGE=1
	export RS_GLOBAL_DTORS=1
fi

#---------------------------------------------------------------------------------------------- 

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

run_tests() {
	local title="$1"
	shift
	if [[ -n $title ]]; then
		$READIES/bin/sep -0
		printf "Running $title:\n\n"
	fi

	if [[ $EXISTING_ENV == 1 ]]; then
		if [[ $REJSON_MODULE ]]; then
			XREDIS_REJSON_ARGS="loadmodule $REJSON_MODULE $REJSON_MODARGS"
		fi

		xredis_conf=$(mktemp "${TMPDIR:-/tmp}/xredis_conf.XXXXXXX")
		rm -f $xredis_conf
		cat <<-EOF > $xredis_conf
			loadmodule $MODULE $MODARGS
			$XREDIS_REJSON_ARGS
			EOF

		rltest_config=$(mktemp "${TMPDIR:-/tmp}/xredis_rltest.XXXXXXX")
		rm -f $rltest_config
		cat <<-EOF > $rltest_config
			--env existing-env
			$RLTEST_ARGS

			EOF

		if [[ $VERBOSE == 1 ]]; then
			echo "External redis-server configuration:"
			cat $xredis_conf
		fi

		$REDIS_SERVER $xredis_conf &
		XREDIS_PID=$!
		echo "External redis-server pid: " $XREDIS_PID

	else
		rltest_config=$(mktemp "${TMPDIR:-/tmp}/rltest.XXXXXXX")
		rm -f $rltest_config
		cat <<-EOF > $rltest_config
			--oss-redis-path=$REDIS_SERVER
			--module $MODULE
			--module-args '$MODARGS'
			$RLTEST_ARGS
			$REJSON_ARGS
			$VALGRIND_ARGS
			$COV_ARGS

			EOF
	fi

	# Use configuration file in the current directory if it exists
	if [[ -n $CONFIG_FILE && -e $CONFIG_FILE ]]; then
		cat $CONFIG_FILE >> $rltest_config
	fi

	if [[ $VERBOSE == 1 ]]; then
		echo "RLTest configuration:"
		cat $rltest_config
	fi

	local E=0
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

	return $E
}

#---------------------------------------------------------------------------------------------- 

E=0

if [[ -z $COORD ]]; then
	{ (run_tests "RediSearch tests"); (( E |= $? )); } || true

elif [[ $COORD == oss ]]; then
	OSS_CLUSTER_ARGS="--env oss-cluster --env-reuse --clear-logs --shards-count 3"

	{ (REJSON=1 MODARGS+=" PARTITIONS AUTO" RLTEST_ARGS+=" ${OSS_CLUSTER_ARGS}" \
	   run_tests "OSS cluster tests"); (( E |= $? )); } || true
	{ (REJSON=1 MODARGS+=" PARTITIONS AUTO; OSS_GLOBAL_PASSWORD password;" \
	   RLTEST_ARGS+=" ${OSS_CLUSTER_ARGS} --oss_password password" \
	   run_tests "OSS cluster tests with password:"); (( E |= $? )); } || true
	{ (REJSON=1 MODARGS+=" PARTITIONS AUTO SAFEMODE" RLTEST_ARGS+=" ${OSS_CLUSTER_ARGS}" \
	   run_tests "OSS cluster tests (safe mode)"); (( E |= $? )); } || true

elif [[ $COORD == rlec ]]; then
	{ (RLTEST_ARGS+=" --env existing-env --existing-env-addr $DOCKER_HOST:$RLEC_PORT" run_tests "tests on RLEC"); (( E |= $? )); } || true
fi

exit $E
