
ROOT=../..
include $(ROOT)/deps/readies/mk/main

DOCKERFILE_STEM=$(ROOT)/build/docker/Dockerfile

# OSNICK=bullseye|bionic|xenial|centos7|centos8
OSNICK ?= bullseye

REPO=redisearch

REDIS_VER=6.2.6
REDISEARCH_VER=master

INT_BRANCHES=2.2 2.0 1.6

ART_DIR=$(ROOT)/bin/artifacts
ART_INT_DIR=/var/opt/redislabs/artifacts

include $(MK)/docker.native
