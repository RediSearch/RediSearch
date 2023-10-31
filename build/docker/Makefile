
ROOT=../..
include $(ROOT)/deps/readies/mk/main

REPO=redisearch

REDIS_VERSION=6.2.13
OSNICK.official=bullseye

INT_BRANCHES=2.8 2.6 2.4 2.2 2.0 1.6
LATEST_BRANCH=2.6
PREVIEW_BRANCH=2.8

ART_DIR=$(ROOT)/bin/artifacts

REJSON_VERSION=2.4
DOCKER_VARS += REJSON_VERSION

include $(MK)/module.docker.rules

verify:
	$(SHOW)DOCKER="$(DEFAULT_TAG)" $(ROOT)/sbin/verify-docker
