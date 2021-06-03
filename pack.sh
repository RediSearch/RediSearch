#!/bin/bash

[[ $IGNERR == 1 ]] || set -e
[[ $VERBOSE == 1 ]] && set -x

error() {
	>&2 echo "$0: There are errors."
	exit 1
}

if [[ -z $_Dbg_DEBUGGER_LEVEL ]]; then
	trap error ERR
fi

export PYTHONWARNINGS=ignore

#----------------------------------------------------------------------------------------------

if [[ $1 == --help || $1 == help ]]; then
	cat <<-END
		Generate RediSearch distribution packages.

		[ARGVARS...] pack.sh [--help|help] [<module-so-path>]

		Argument variables:
		VERBOSE=1     Print commands
		IGNERR=1      Do not abort on error

		RAMP=1              Generate RAMP package
		DEPS=1              Generate dependency packages
		RELEASE=1           Generate "release" packages (artifacts/release/)
		SNAPSHOT=1          Generate "shapshot" packages (artifacts/snapshot/)
		JUST_PRINT=1        Only print package names, do not generate

		PACKAGE_NAME=name   Package stem name
		VARIANT=name        Build variant (empty for standard packages)
		BRANCH=name         Branch name for snapshot packages
		GITSHA=1            Append Git SHA to shapshot package names

		ARTDIR=dir          Directory in which packages are created
		
		RAMP_YAML=path      RAMP configuration file path
		RAMP_ARGS=args      Extra arguments to RAMP

	END
	exit 0
fi

#----------------------------------------------------------------------------------------------

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$HERE
READIES=$ROOT/deps/readies
. $READIES/shibumi/functions
CURDIR="$PWD"

#----------------------------------------------------------------------------------------------

ARCH=$($READIES/bin/platform --arch)
[[ $ARCH == x64 ]] && ARCH="x86_64"

OS=$($READIES/bin/platform --os)
[[ $OS == linux ]] && OS="Linux"

OSNICK=$($READIES/bin/platform --osnick)
[[ $OSNICK == trusty ]] && OSNICK=ubuntu14.04
[[ $OSNICK == xenial ]] && OSNICK=ubuntu16.04
[[ $OSNICK == bionic ]] && OSNICK=ubuntu18.04
[[ $OSNICK == centos7 ]] && OSNICK=rhel7

#----------------------------------------------------------------------------------------------

MODULE_SO="$1"

RAMP=${RAMP:-1}
DEPS=${DEPS:-1}

RELEASE=${RELEASE:-1}
SNAPSHOT=${SNAPSHOT:-1}

if [[ $JUST_PRINT != 1 ]]; then
	[[ -z $ARTDIR ]] && { echo ARTDIR undefined; exit 1; }
	ARTDIR=$(realpath $ARTDIR)
fi

#----------------------------------------------------------------------------------------------

PACKAGE_NAME=${PACKAGE_NAME:-redisearch-oss}

DEP_NAMES="debug"

#----------------------------------------------------------------------------------------------

pack_ramp() {
	cd $ROOT

	local platform="$OS-$OSNICK-$ARCH"
	local stem=${PACKAGE_NAME}.${platform}

	if [[ $SNAPSHOT == 0 ]]; then
		local verspec=${SEMVER}${VARIANT}
		local packdir=.
		local s3base=""
	else
		local verspec=${BRANCH}${VARIANT}-snapshot
		local packdir=snapshots
		local s3base=snapshots/
	fi
	
	local fq_package=$stem.${verspec}.zip

	[[ ! -d $ARTDIR/$packdir ]] && mkdir -p $ARTDIR/$packdir

	local packfile=$ARTDIR/$packdir/$fq_package

	local xtx_vars=""
	for dep in $DEP_NAMES; do
		eval "export NAME_${dep}=${PACKAGE_NAME}_${dep}"
		local dep_fname=${PACKAGE_NAME}-${dep}.${platform}.${verspec}.tgz
		eval "export PATH_${dep}=${s3base}${dep_fname}"
		local dep_sha256="$ARTDIR/$packdir/${dep_fname}.sha256"
		eval "export SHA256_${dep}=$(cat $dep_sha256)"

		xtx_vars+=" -e NAME_$dep -e PATH_$dep -e SHA256_$dep"
	done
	
	if [[ -z $RAMP_YAML ]]; then
		RAMP_YAML=$ROOT/ramp.yml
	fi
	
	python2 $READIES/bin/xtx \
		$xtx_vars \
		-e NUMVER -e SEMVER \
		$RAMP_YAML > /tmp/ramp.yml

	local ramp="$(command -v python2) -m RAMP.ramp"
	rm -f /tmp/ramp.fname
	
	# CURDIR is required so ramp will detect the right git commit
	cd $CURDIR
	$ramp pack -m /tmp/ramp.yml $RAMP_ARGS --packname-file /tmp/ramp.fname --verbose --debug \
		-o $packfile $MODULE_SO >/tmp/ramp.err 2>&1 || true

	if [[ ! -e $packfile ]]; then
		>&2 echo "Error generating RAMP file:"
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
	
	local depdir=$(cat $ARTDIR/$dep.dir)

	local platform="$OS-$OSNICK-$ARCH"
	
	local stem=${PACKAGE_NAME}-${dep}.${platform}
	local fq_package=$stem.${SEMVER}${VARIANT}.tgz
	local tar_path=$ARTDIR/$fq_package
	local dep_prefix_dir=$(cat $ARTDIR/$dep.prefix)
	
	{ cd $depdir ;\
	  cat $ARTDIR/$dep.files | \
	  xargs tar -c --sort=name --owner=root:0 --group=root:0 --mtime='UTC 1970-01-01' --transform "s,^,$dep_prefix_dir," 2>> /tmp/pack.err | \
	  gzip -n - > $tar_path ; E=$?; } || true
	sha256sum $tar_path | gawk '{print $1}' > $tar_path.sha256

	mkdir -p $ARTDIR/snapshots
	cd $ARTDIR/snapshots
	if [[ ! -z $BRANCH ]]; then
		local snap_package=$stem.${BRANCH}${VARIANT}-snapshot.tgz
		ln -sf ../$fq_package $snap_package
		ln -sf ../$fq_package.sha256 $snap_package.sha256
	fi
}

#----------------------------------------------------------------------------------------------

NUMVER=$(NUMERIC=1 $ROOT/getver)
SEMVER=$($ROOT/getver)

[[ -z $BRANCH ]] && BRANCH=${CIRCLE_BRANCH:-`git rev-parse --abbrev-ref HEAD`}
BRANCH=${BRANCH//[^A-Za-z0-9._-]/_}
if [[ $GITSHA == 1 ]]; then
	GIT_COMMIT=$(git describe --always --abbrev=7 --dirty="+" 2>/dev/null || git rev-parse --short HEAD)
	BRANCH="${BRANCH}-${GIT_COMMIT}"
fi

if [[ ! -z $VARIANT ]]; then
	VARIANT=-${VARIANT}
fi

RELEASE_ramp=${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.$SEMVER${VARIANT}.zip
SNAPSHOT_ramp=${PACKAGE_NAME}.$OS-$OSNICK-$ARCH.${BRANCH}${VARIANT}-snapshot.zip

if [[ $JUST_PRINT == 1 ]]; then
	if [[ $RAMP == 1 ]]; then
		[[ $RELEASE == 1 ]] && echo $RELEASE_ramp
		[[ $SNAPSHOT == 1 ]] && echo $SNAPSHOT_ramp
	fi
	# if [[ $DEPS == 1 ]]; then
	# 	[[ $RELEASE == 1 ]] && echo $RELEASE_deps
	# 	[[ $SNAPSHOT == 1 ]] && echo $SNAPSHOT_deps
	# fi
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
		(pack_deps $dep)
	done
	echo "Done."
fi

#----------------------------------------------------------------------------------------------

if [[ $RAMP == 1 ]]; then
	if ! command -v redis-server > /dev/null; then
		>&2 echo "$0: Cannot find redis-server. Aborting."
		exit 1
	fi

	echo "Building RAMP files ..."

	[[ -z $MODULE_SO ]] && >&2 echo "$0: Nothing to pack. Aborting." && exit 1
	[[ ! -f $MODULE_SO ]] && >&2 echo "$0: $MODULE_SO does not exist. Aborting." && exit 1
	MODULE_SO=$(realpath $MODULE_SO)

	[[ $RELEASE == 1 ]] && SNAPSHOT=0 pack_ramp
	[[ $SNAPSHOT == 1 ]] && pack_ramp
	
	echo "Done."
fi
