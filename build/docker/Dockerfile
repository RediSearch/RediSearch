# BUILD redisfab/redisearch:${VERSION}-${ARCH}-${OSNICK}

ARG REDIS_VER=6.2.6

# OSNICK=bullseye|centos7|centos8|xenial|bionic
ARG OSNICK=bullseye

# OS=debian:bullseye-slim|centos:7|centos:8|ubuntu:xenial|ubuntu:bionic
ARG OS=debian:bullseye-slim

# ARCH=x64|arm64v8|arm32v7
ARG ARCH=x64

ARG PACK=0
ARG TEST=0

ARG GIT_SHA
ARG GIT_VERSPEC

ARG REJSON_BRANCH=master

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK} AS redis
FROM ${OS} AS builder

ARG OSNICK
ARG OS
ARG ARCH
ARG REDIS_VER
ARG PACK
ARG TEST
ARG GIT_SHA
ARG GIT_VERSPEC
ARG REJSON_BRANCH

RUN echo "Building for ${OSNICK} (${OS}) for ${ARCH} [with Redis ${REDIS_VER}]"

WORKDIR /build
COPY --from=redis /usr/local/ /usr/local/

ADD . /build

RUN ./deps/readies/bin/getupdates
RUN ./deps/readies/bin/getpy2
RUN ./sbin/system-setup.py

RUN /usr/local/bin/redis-server --version
RUN bash -l -c "make fetch"

RUN bash -l -c "make build WITH_TESTS=1 SHOW=1"
RUN set -e ;\
    if [ "$PACK" = "1" ]; then \
        bash -l -c "make build LITE=1 SHOW=1" ;\
        bash -l -c "make build COORD=rlec SHOW=1" ;\
    fi
RUN set -e ;\
    if [ "$TEST" = "1" ]; then \
        bash -l -c "make build WITH_TESTS=1 COORD=oss SHOW=1" ;\
    fi

RUN mkdir -p bin/artifacts
RUN set -e ;\
    if [ "$PACK" = "1" ]; then \
        bash -l -c "make pack COORD=rlec" ;\
		bash -l -c "make pack LITE=1" ;\
		echo "Artifacts:" ;\
		du -ah --apparent-size /build/bin/artifacts ;\
    fi
RUN set -e ;\
    if [ "$TEST" = "1" ]; then \
        bash -l -c "TEST= make test" ;\
		bash -l -c "TEST= make test COORD=oss" ;\
        cd /build/tests/pytests/logs ;\
        rm -f *.aof *.rdb ;\
        tar -czf /build/bin/artifacts/tests-pytests-logs-${ARCH}-${OSNICK}.tgz . ;\
    fi

#----------------------------------------------------------------------------------------------
FROM redisfab/rejson:${REJSON_BRANCH}-${ARCH}-${OSNICK} AS json
FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK}

ARG OSNICK
ARG OS
ARG ARCH
ARG REDIS_VER

WORKDIR /data

ENV LIBDIR /usr/lib/redis/modules/
RUN mkdir -p "$LIBDIR"

RUN mkdir -p /var/opt/redislabs/artifacts
RUN chown -R redis:redis /var/opt/redislabs
COPY --from=builder /build/bin/linux-x64-release/search/redisearch.so* "$LIBDIR"
RUN true
COPY --from=json    /usr/lib/redis/modules/rejson.so* "$LIBDIR"
RUN true
COPY --from=builder /build/bin/artifacts/ /var/opt/redislabs/artifacts

CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redisearch.so", "--loadmodule", "/usr/lib/redis/modules/rejson.so"]
