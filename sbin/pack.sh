#!/bin/bash

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $HERE/.. && pwd)
SBIN=$ROOT/sbin

GET_PLATFORM="$SBIN/get-platform"

realpath() {
  local target="$1"
  if [ -z "$target" ]; then
    return 1
  fi
  (
    cd "$(dirname "$target")" || exit 1
    echo "$(pwd -P)/$(basename "$target")"
  )
}
eprint() { echo "$@" >&2; }


export PYTHONWARNINGS=ignore

cd $ROOT

#----------------------------------------------------------------------------------------------

if [[ $1 == --help || $1 == help || $HELP == 1 ]]; then
	cat <<-END
		Generate RediSearch distribution packages.

		[ARGVARS...] pack.sh [--help|help] [<module-so-path>]

		Argument variables:
		RAMP=0|1            Build RAMP package

		MODULE_NAME=name    Module name (default: redisearch)
		PACKAGE_NAME=name   Package stem name

		BRANCH=name         Branch name for snapshot packages
		WITH_GITSHA=1       Append Git SHA to snapshot package names
		VARIANT=name        Build variant
		RAMP_VARIANT=name   RAMP variant (e.g. ramp-{name}.yml)

		ARTDIR=dir          Directory in which packages are created (default: bin/artifacts)

		RAMP_YAML=path      RAMP configuration file path
		RAMP_ARGS=args      Extra arguments to RAMP

		JUST_PRINT=1        Only print package names, do not generate
		VERBOSE=1           Print commands
		HELP=1              Show help

	END
	exit 0
fi

#----------------------------------------------------------------------------------------------

# RLEC naming conventions

ARCH=$($GET_PLATFORM --arch)

OS=$($GET_PLATFORM --os)

OSNICK=$($GET_PLATFORM --version-artifact)


PLATFORM="$OS-$OSNICK-$ARCH"

#----------------------------------------------------------------------------------------------

MODULE="$1"

RAMP=${RAMP:-1}


[[ -z $ARTDIR ]] && ARTDIR=bin/artifacts
mkdir -p $ARTDIR $ARTDIR/snapshots
ARTDIR=$(cd $ARTDIR && pwd)

#----------------------------------------------------------------------------------------------

MODULE_NAME=${MODULE_NAME:-redisearch}
PACKAGE_NAME=${PACKAGE_NAME:-redisearch-oss}

RAMP_CMD="python3 -m RAMP.ramp"

#----------------------------------------------------------------------------------------------

run_ramp_pack() {
	local input="$1"
	local output="$2"

	$RAMP_CMD pack -m /tmp/ramp.yml \
		$RAMP_ARGS \
		-n "$MODULE_NAME" \
		--verbose \
		--debug \
		--packname-file /tmp/ramp.fname \
		-o "$output" \
		"$input" \
		>/tmp/ramp.err 2>&1 || true


	if [[ ! -e "$output" ]]; then
		eprint "Error generating RAMP file:"
		>&2 cat /tmp/ramp.err
		exit 1
	else
		local packname
		packname="$(cat /tmp/ramp.fname)"
		echo "# Created $(realpath "$packname")"
	fi
}

pack_ramp() {
	cd $ROOT

	local stem=${PACKAGE_NAME}.${PLATFORM}
	local stem_debug=${PACKAGE_NAME}.debug.${PLATFORM}

	local verspec=${BRANCH}${VARIANT}
	local packdir=snapshots
	local s3base=snapshots/

	local fq_package=$stem.${verspec}.zip
	local fq_package_debug=$stem_debug.${verspec}.zip

	mkdir -p $ARTDIR/$packdir

	local packfile=$ARTDIR/$packdir/$fq_package
	local packfile_debug=$ARTDIR/$packdir/$fq_package_debug

	if [[ -n $RAMP_YAML ]]; then
		RAMP_YAML="$(realpath $RAMP_YAML)"
	elif [[ -z $RAMP_VARIANT ]]; then
		RAMP_YAML="$ROOT/pack/ramp.yml"
	else
		RAMP_YAML="$ROOT/pack/ramp${RAMP_VARIANT:+-$RAMP_VARIANT}.yml"
	fi

	if [[ $VERBOSE == 1 ]]; then
		echo "# ramp.yml:"
	fi

	rm -f /tmp/ramp.fname $packfile

	run_ramp_pack "$MODULE" "$packfile"

	if [[ -f "$MODULE.debug" ]]; then
		run_ramp_pack "$MODULE.debug" "$packfile_debug"
	fi

	cd "$ROOT"
}


#----------------------------------------------------------------------------------------------

# SEMVER - Semantic version (format: major.minor.patch like 2.6.3)
# Used for human-readable package naming and release version identification
SEMVER="$($SBIN/getver)"


git_config_add_ifnx() {
	local key="$1"
	local val="$2"
	if [[ -z $(git config --global --get $key $val) ]]; then
		git config --global --add $key $val
	fi
}

if [[ -z $BRANCH ]]; then
	git_config_add_ifnx safe.directory $ROOT
	BRANCH=$(git rev-parse --abbrev-ref HEAD)
	# this happens of detached HEAD
	if [[ $BRANCH == HEAD ]]; then
		BRANCH="$SEMVER"
	fi
fi
BRANCH=${BRANCH//[^A-Za-z0-9._-]/_}
if [[ $WITH_GITSHA == 1 ]]; then
	git_config_add_ifnx safe.directory $ROOT
	GIT_COMMIT=$(git rev-parse --short HEAD)
	BRANCH="${BRANCH}-${GIT_COMMIT}"
fi

#----------------------------------------------------------------------------------------------

SNAPSHOT_ramp=${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.${BRANCH}${VARIANT}.zip

#----------------------------------------------------------------------------------------------

if [[ $JUST_PRINT == 1 ]]; then
	if [[ $RAMP == 1 ]]; then
		echo $SNAPSHOT_ramp
	fi
	exit 0
fi

cd $ROOT

if [[ $RAMP == 1 ]]; then
	if ! command -v redis-server > /dev/null; then
		eprint "Cannot find redis-server. Aborting."
		exit 1
	fi

	echo "# Building RAMP $RAMP_VARIANT files ..."

	[[ -z $MODULE ]] && { eprint "Nothing to pack. Aborting."; exit 1; }
	[[ ! -f $MODULE ]] && { eprint "$MODULE does not exist. Aborting."; exit 1; }
	MODULE=$(realpath $MODULE)

	pack_ramp

	echo "# Done."
fi

if [[ $VERBOSE == 1 ]]; then
	echo "# Artifacts:"
	if [[ $OSNICK == alpine3 ]]; then
		du -ah $ARTDIR
	else
		du -ah --apparent-size $ARTDIR
	fi
fi

exit 0
