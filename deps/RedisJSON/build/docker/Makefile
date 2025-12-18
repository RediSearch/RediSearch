
ROOT=../..

include $(ROOT)/deps/readies/mk/main

REPO=rejson

REDIS_VERSION=7.4

OSNICK.official=bullseye

INT_BRANCHES=2.6 2.4 2.2 2.0 1.2 1.0
LATEST_BRANCH=2.6
PREVIEW_BRANCH=2.6

ART_DIR=$(ROOT)/bin/artifacts

REDISEARCH_VERSION=master
DOCKER_VARS += REDISEARCH_VERSION NO_SEARCH
NO_SEARCH=1

include $(MK)/module.docker.rules
