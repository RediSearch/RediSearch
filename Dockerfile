FROM redislabsmodules/rmbuilder:latest as builder

# Build the source
ARG GIT_DESCRIBE_VERSION
ADD . /RSBUILD
WORKDIR /RSBUILD
RUN set -ex;\
    ./srcutil/get_gtest.sh; \
    rm -rf docker-build; \
    mkdir docker-build; \
    cd docker-build; \
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ../ -DGIT_DESCRIBE_VERSION="$GIT_DESCRIBE_VERSION" -DRS_RUN_TESTS=ON; \
    make -j4; \
    pip install git+https://github.com/RedisLabsModules/RLTest; \
    pip install redis-py-cluster; \
    make test;

# Package the runner
FROM redis:latest
ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN set -ex;\
    mkdir -p "$LIBDIR";

COPY --from=builder /RSBUILD/docker-build/redisearch.so  "$LIBDIR"
CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redisearch.so"]
