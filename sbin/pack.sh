#!/bin/bash

# [[ $VERBOSE == 1 ]] && set -x

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $HERE/.. && pwd)
export READIES=$ROOT/deps/readies
. $READIES/shibumi/defs
SBIN=$ROOT/sbin

export PYTHONWARNINGS=ignore

cd $ROOT

#----------------------------------------------------------------------------------------------

if [[ $1 == --help || $1 == help || $HELP == 1 ]]; then
	cat <<-END
		Generate RediSearch distribution packages.

		[ARGVARS...] pack.sh [--help|help] [<module-so-path>]

		Argument variables:
		RAMP=1              Generate RAMP package
		DEPS=1              Generate dependency packages
		RELEASE=1           Generate "release" packages (artifacts/release/)
		SNAPSHOT=1          Generate "shapshot" packages (artifacts/snapshot/)
		JUST_PRINT=1        Only print package names, do not generate

		MODULE_NAME=name    Module name (default: redisearch)
		PACKAGE_NAME=name   Package stem name

		BRANCH=name         Branch name for snapshot packages
		VERSION=ver         Version for release packages
		WITH_GITSHA=1       Append Git SHA to shapshot package names
		VARIANT=name        Build variant (empty for standard packages)

		ARTDIR=dir          Directory in which packages are created (default: bin/artifacts)
		
		RAMP_YAML=path      RAMP configuration file path
		RAMP_ARGS=args      Extra arguments to RAMP

		VERBOSE=1           Print commands
		IGNERR=1            Do not abort on error

	END
	exit 0
fi

#----------------------------------------------------------------------------------------------

ARCH=$($READIES/bin/platform --arch)
[[ $ARCH == x64 ]] && ARCH="x86_64"

OS=$($READIES/bin/platform --os)
[[ $OS == linux ]] && OS="Linux"

OSNICK=$($READIES/bin/platform --osnick)
[[ $OSNICK == trusty ]] && OSNICK=ubuntu14.04
[[ $OSNICK == xenial ]] && OSNICK=ubuntu16.04
[[ $OSNICK == bionic ]] && OSNICK=ubuntu18.04
[[ $OSNICK == focal ]] && OSNICK=ubuntu20.04
[[ $OSNICK == centos7 ]] && OSNICK=rhel7
[[ $OSNICK == centos8 ]] && OSNICK=rhel8
[[ $OSNICK == ol8 ]] && OSNICK=rhel8

PLATFORM="$OS-$OSNICK-$ARCH"

#----------------------------------------------------------------------------------------------

MODULE_SO="$1"

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

DEP_NAMES="debug"

#----------------------------------------------------------------------------------------------

pack_ramp() {
	cd $ROOT

	local stem=${PACKAGE_NAME}.${PLATFORM}

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

	[[ ! -d $ARTDIR/$packdir ]] && mkdir -p $ARTDIR/$packdir

	local packfile=$ARTDIR/$packdir/$fq_package

	local xtx_vars=""
	for dep in $DEP_NAMES; do
		eval "export NAME_${dep}=${PACKAGE_NAME}_${dep}"
		local dep_fname="${PACKAGE_NAME}.${dep}.${PLATFORM}.${verspec}.tgz"
		eval "export PATH_${dep}=${s3base}${dep_fname}"
		local dep_sha256="$ARTDIR/$packdir/${dep_fname}.sha256"
		eval "export SHA256_${dep}=$(cat $dep_sha256)"

		xtx_vars+=" -e NAME_$dep -e PATH_$dep -e SHA256_$dep"
	done
	
	if [[ -z $RAMP_YAML ]]; then
		RAMP_YAML=$ROOT/ramp.yml
	fi
	
	python3 $READIES/bin/xtx \
		$xtx_vars \
		-e NUMVER -e SEMVER \
		$RAMP_YAML > /tmp/ramp.yml

	local ramp="$(command -v python3) -m RAMP.ramp"
	rm -f /tmp/ramp.fname
	
	# ROOT is required so ramp will detect the right git commit
	cd $ROOT
	$ramp pack -m /tmp/ramp.yml $RAMP_ARGS -n $MODULE_NAME --verbose --debug \
		--packname-file /tmp/ramp.fname -o $packfile \
		$MODULE_SO >/tmp/ramp.err 2>&1 || true

	if [[ ! -e $packfile ]]; then
		eprint "Error generating RAMP file:"
		>&2 cat /tmp/ramp.err
		exit 1
	else
		local packname=`cat /tmp/ramp.fname`
	fi

	echo "Created $packname"
	cd $ROOT
}

#----------------------------------------------------------------------------------------------

pack_deps() {
	local dep="$1"
	
	cd $ROOT
	
	local stem=${PACKAGE_NAME}.${dep}.${PLATFORM}
	local verspec=${SEMVER}${VARIANT}
	local fq_package=$stem.${verspec}.tgz

	local depdir=$(cat $ARTDIR/$dep.dir)
	local tar_path=$ARTDIR/$fq_package
	local dep_prefix_dir=$(cat $ARTDIR/$dep.prefix)
	
	rm -f $tar_path
	{ cd $depdir ;\
	  cat $ARTDIR/$dep.files | \
	  xargs tar -c --sort=name --owner=root:0 --group=root:0 --mtime='UTC 1970-01-01' --transform "s,^,$dep_prefix_dir," 2>> /tmp/pack.err | \
	  gzip -n - > $tar_path ; E=$?; } || true
	if [[ ! -e $tar_path || -z $(tar tzf $tar_path) ]]; then
		eprint "Count not create $tar_path. Aborting."
		rm -f $tar_path
		exit 1
	fi
	sha256sum $tar_path | gawk '{print $1}' > $tar_path.sha256

	mkdir -p $ARTDIR/snapshots
	cd $ARTDIR/snapshots
	if [[ ! -z $BRANCH ]]; then
		local snap_package=$stem.${BRANCH}${VARIANT}.tgz
		ln -sf ../$fq_package $snap_package
		ln -sf ../$fq_package.sha256 $snap_package.sha256
	fi
}

#----------------------------------------------------------------------------------------------

NUMVER=$(NUMERIC=1 $SBIN/getver)
SEMVER=$($SBIN/getver)

if [[ ! -z $VARIANT ]]; then
	VARIANT=-${VARIANT}
fi

#----------------------------------------------------------------------------------------------

if [[ -z $BRANCH ]]; then
	BRANCH=$(git rev-parse --abbrev-ref HEAD)
	# this happens of detached HEAD
	if [[ $BRANCH == HEAD ]]; then
		BRANCH="$SEMVER"
	fi
fi
BRANCH=${BRANCH//[^A-Za-z0-9._-]/_}
if [[ $WITH_GITSHA == 1 ]]; then
	GIT_COMMIT=$(git rev-parse --short HEAD)
	BRANCH="${BRANCH}-${GIT_COMMIT}"
fi

#----------------------------------------------------------------------------------------------

RELEASE_ramp=${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.$SEMVER${VARIANT}.zip
SNAPSHOT_ramp=${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.${BRANCH}${VARIANT}.zip

RELEASE_deps=
SNAPSHOT_deps=
for dep in $DEP_NAMES; do
	RELEASE_deps+=" ${PACKAGE_NAME}.${dep}.$OS-$OSNICK-$ARCH.$SEMVER${VARIANT}.tgz"
	SNAPSHOT_deps+=" ${PACKAGE_NAME}.${dep}.$OS-$OSNICK-$ARCH.${BRANCH}${VARIANT}.tgz"
done

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

#----------------------------------------------------------------------------------------------

mkdir -p $ARTDIR

if [[ $DEPS == 1 ]]; then
	# set up `debug` dep
	echo $(dirname $(realpath $MODULE_SO)) > $ARTDIR/debug.dir
	echo $(basename $(realpath $MODULE_SO)).debug > $ARTDIR/debug.files
	echo "" > $ARTDIR/debug.prefix

	echo "Building dependencies ..."
	for dep in $DEP_NAMES; do
		if [[ $OS != macos ]]; then
			pack_deps $dep
		fi
	done
	echo "Done."
fi

#----------------------------------------------------------------------------------------------

cd $ROOT

if [[ $RAMP == 1 ]]; then
	if ! command -v redis-server > /dev/null; then
		eprint "Cannot find redis-server. Aborting."
		exit 1
	fi

	echo "Building RAMP files ..."

	[[ -z $MODULE_SO ]] && { eprint "Nothing to pack. Aborting."; exit 1; }
	[[ ! -f $MODULE_SO ]] && { eprint "$MODULE_SO does not exist. Aborting."; exit 1; }
	MODULE_SO=$(realpath $MODULE_SO)

	[[ $RELEASE == 1 ]] && SNAPSHOT=0 pack_ramp
	[[ $SNAPSHOT == 1 ]] && pack_ramp
	
	echo "Done."
fi
