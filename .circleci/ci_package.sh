#!/bin/bash
set -x
set -e
# TODO: The name is not always `.so`


if [ -z "$DISTDIR" ]; then
    DISTDIR=/workspace
fi

if [ -z "$MODULE_SO" ]; then
    MODULE_SO=$BUILD_DIR/redisearch.so
fi

if [ -z "$RAMP_YML" ]; then
    RAMP_YML="ramp.yml"
fi

# Install my fork of RAMP- need to merge it first
pip install git+https://github.com/RedisLabs/RAMP@master --upgrade

if [ -z "$DIST_SUFFIX" ]; then
    if [ -e /etc/redhat-release ]; then
        DIST_SUFFIX=-rha
    elif [ -e /etc/debian_version ]; then
        DIST_SUFFIX=""
    else
        DIST_SUFFIX="" # WAT?
    fi
fi


function do_build {
mode=$1
CIRCLE_BRANCH=`echo $CIRCLE_BRANCH|tr / -`
case "$mode" in
    'release')
        format={os}$DIST_SUFFIX-{architecture}.{semantic_version}
        subdir=release
        ;;
    'snapshot')
        format={os}$DIST_SUFFIX-{architecture}.$CIRCLE_BRANCH-snapshot
        subdir=snapshot
        ;;
    *)
        echo 'No such mode!'
        exit 1
esac
distdir=$DISTDIR/$subdir
mkdir -p $distdir
outname_base=$distdir/$PACKAGE_NAME.$format
echo "Old outname base: $outname_base"
outname_base=$(ramp pack -m ${RAMP_YML} -o $outname_base --print-filename-only ${RAMP_ARGS} $MODULE_SO)
echo "Output is $outname_base"
ramp pack -m ${RAMP_YML} -o $outname_base.zip $MODULE_SO
return 0
}

do_build release
do_build snapshot
