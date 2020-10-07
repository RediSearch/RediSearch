# BUILD redisfab/redisearch:${VERSION}-${ARCH}-${OSNICK}-clang

ARG CLANG_VER=12

ARG OSNICK=stretch
ARG ARCH=x64

ARG GIT_DESCRIBE_VERSION

#----------------------------------------------------------------------------------------------
FROM redisfab/clang:${CLANG_VER}-${OSNICK}-${ARCH}

ARG CLANG_VER
ARG OSNICK
ARG OS
ARG ARCH
ARG GIT_DESCRIBE_VERSION

WORKDIR /build

ADD . /build

RUN ./.circleci/ci_get_deps.sh
RUN ./tests/sanitizer/build.sh

CMD [ '/bin/bash', '-c', '/build/tests/sanitizer/test.sh' ]
