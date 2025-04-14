#!/bin/bash

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $HERE/.. && pwd)
SBIN=$ROOT/sbin

GET_PLATFORM="$SBIN/get-platform"
XTX="$SBIN/xtx"
SHIBUMI="$SBIN/shibumi"

realpath() { python3 "$SHIBUMI" --realpath "$@"; }
runn() { python3 "$SHIBUMI" --runn "$@"; }
eprint() { python3 "$SHIBUMI" --eprint "$@"; }


export PYTHONWARNINGS=ignore

cd $ROOT

#----------------------------------------------------------------------------------------------

if [[ $1 == --help || $1 == help || $HELP == 1 ]]; then
	cat <<-END
		Generate RediSearch distribution packages.

		[ARGVARS...] pack.sh [--help|help] [<module-so-path>]

		Argument variables:
		RAMP=0|1            Build RAMP package
		DEPS=0|1            Build dependencies files
		RELEASE=1           Generate "release" packages (artifacts/release/)
		SNAPSHOT=1          Generate "snapshot" packages (artifacts/snapshot/)

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
		NOP=1               Print commands, do not execute

	END
	exit 0
fi

#----------------------------------------------------------------------------------------------

OP=""
[[ $NOP == 1 ]] && OP=echo

# RLEC naming conventions

ARCH=$($GET_PLATFORM --arch)

OS=$($GET_PLATFORM --os)

OSNICK=$($GET_PLATFORM --version-artifact)


PLATFORM="$OS-$OSNICK-$ARCH"

#----------------------------------------------------------------------------------------------

MODULE="$1"

RAMP=${RAMP:-1}
DEPS=${DEPS:-1}

RELEASE=${RELEASE:-1}
SNAPSHOT=${SNAPSHOT:-1}

[[ -z $ARTDIR ]] && ARTDIR=bin/artifacts
mkdir -p $ARTDIR $ARTDIR/snapshots
ARTDIR=$(cd $ARTDIR && pwd)

#----------------------------------------------------------------------------------------------

MODULE_NAME=${MODULE_NAME:-redisearch}
PACKAGE_NAME=${PACKAGE_NAME:-redisearch-oss}

RAMP_CMD="python3 -m RAMP.ramp"

#----------------------------------------------------------------------------------------------

pack_ramp() {
	cd $ROOT

	local stem=${PACKAGE_NAME}.${PLATFORM}
	local stem_debug=${PACKAGE_NAME}.debug.${PLATFORM}

	if [[ $SNAPSHOT == 0 ]]; then
		local verspec=${SEMVER}${VARIANT}
		local packdir=.
		local s3base=""
	else
		local verspec=${BRANCH}${VARIANT}
		local packdir=snapshots
		local s3base=snapshots/
	fi

	local fq_package=$stem.${verspec}.zip
	local fq_package_debug=$stem_debug.${verspec}.zip

	mkdir -p $ARTDIR/$packdir

	local packfile=$ARTDIR/$packdir/$fq_package
	local packfile_debug=$ARTDIR/$packdir/$fq_package_debug

	local xtx_vars=""

	if [[ -n $RAMP_YAML ]]; then
		RAMP_YAML="$(realpath $RAMP_YAML)"
	elif [[ -z $RAMP_VARIANT ]]; then
		RAMP_YAML="$ROOT/pack/ramp.yml"
	else
		# ${RAMP_VARIANT:+-$RAMP_VARIANT} adds a dash-prefixed variant only if RAMP_VARIANT is set and non-empty.
		RAMP_YAML="$ROOT/pack/ramp${RAMP_VARIANT:+-$RAMP_VARIANT}.yml"
	fi

	python3 $XTX \
		$xtx_vars \
		-e NUMVER -e SEMVER \
		$RAMP_YAML > /tmp/ramp.yml
	if [[ $VERBOSE == 1 ]]; then
		echo "# ramp.yml:"
		cat /tmp/ramp.yml
	fi

	runn rm -f /tmp/ramp.fname $packfile

	# ROOT is required so ramp will detect the right git commit
	cd $ROOT
	runn @ <<-EOF
		$RAMP_CMD pack -m /tmp/ramp.yml \
			$RAMP_ARGS \
			-n $MODULE_NAME \
			--verbose \
			--debug \
			--packname-file /tmp/ramp.fname \
			-o $packfile \
			$MODULE \
			>/tmp/ramp.err 2>&1 || true
		EOF

	if [[ $NOP != 1 ]]; then
		if [[ ! -e $packfile ]]; then
			eprint "Error generating RAMP file:"
			>&2 cat /tmp/ramp.err
			exit 1
		else
			local packname=`cat /tmp/ramp.fname`
			echo "# Created $(realpath $packname)"
		fi
	fi

	if [[ -f $MODULE.debug ]]; then
		runn @ <<-EOF
			$RAMP_CMD pack -m /tmp/ramp.yml \
				$RAMP_ARGS \
				-n $MODULE_NAME \
				--verbose \
				--debug \
				--packname-file /tmp/ramp.fname \
				-o $packfile_debug \
				$MODULE.debug \
				>/tmp/ramp.err 2>&1 || true
			EOF

		if [[ $NOP != 1 ]]; then
			if [[ ! -e $packfile_debug ]]; then
				eprint "Error generating RAMP file:"
				>&2 cat /tmp/ramp.err
				exit 1
			else
				local packname=`cat /tmp/ramp.fname`
				echo "# Created $(realpath $packname)"
			fi
		fi
	fi

	cd $ROOT
}

#----------------------------------------------------------------------------------------------
# NUMVER - Numeric module version (format: single integer like 20603)
# Used for Redis module API version compatibility and internal versioning
NUMVER="$(NUMERIC=1 $SBIN/getver)"

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

RELEASE_ramp=${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.$SEMVER${VARIANT}.zip
SNAPSHOT_ramp=${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.${BRANCH}${VARIANT}.zip

RELEASE_deps=
SNAPSHOT_deps=

#----------------------------------------------------------------------------------------------

if [[ $JUST_PRINT == 1 ]]; then
	if [[ $RAMP == 1 ]]; then
		[[ $RELEASE == 1 ]] && echo $RELEASE_ramp
		[[ $SNAPSHOT == 1 ]] && echo $SNAPSHOT_ramp
	fi
	if [[ $DEPS == 1 ]]; then
		[[ $RELEASE == 1 ]] && echo $RELEASE_deps
		[[ $SNAPSHOT == 1 ]] && echo $SNAPSHOT_deps
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

	[[ $RELEASE == 1 ]] && SNAPSHOT=0 pack_ramp
	[[ $SNAPSHOT == 1 ]] && pack_ramp

	echo "# Done."
fi

if [[ $VERBOSE == 1 ]]; then
	echo "# Artifacts:"
	if [[ $OSNICK == alpine3 ]]; then
		$OP du -ah $ARTDIR
	else
		$OP du -ah --apparent-size $ARTDIR
	fi
fi

exit 0
