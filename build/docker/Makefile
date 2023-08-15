
ROOT=../..
include $(ROOT)/deps/readies/mk/main

REPO=redisearch

REDIS_VERSION=7.2.0
OSNICK.official=bullseye

INT_BRANCHES=2.8 2.6 2.4 2.2 2.0 1.6
LATEST_BRANCH=2.6
PREVIEW_BRANCH=2.8

ART_DIR=$(ROOT)/bin/artifacts

REJSON_VERSION=master
DOCKER_VARS += REJSON_VERSION REDISEARCH_MT_BUILD

include $(MK)/module.docker.rules

verify:
	$(SHOW)DOCKER="$(DEFAULT_TAG)" $(ROOT)/sbin/verify-docker
