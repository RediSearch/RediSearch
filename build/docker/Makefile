
ROOT=../..
include $(ROOT)/deps/readies/mk/main

REPO=redisearch

REDIS_VERSION=6.2.6
OSNICK.official=bullseye

INT_BRANCHES=2.2 2.0 1.6
LATEST_BRANCH=2.2
PREVIEW_BRANCH=2.2

ART_DIR=$(ROOT)/bin/artifacts

REJSON_VERSION=master
DOCKER_VARS += REJSON_VERSION

include $(MK)/module.docker.rules
