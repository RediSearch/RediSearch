FROM redislabsmodules/rmbuilder:latest as builder

# Build the source
ADD . /RSBUILD
WORKDIR /RSBUILD
RUN set -ex;\
    rm -rf docker-build; \
    mkdir docker-build; \
    cd docker-build; \
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ../; \
    make -j4; \
    pip install git+https://github.com/RedisLabs/rmtest@2.0; \
    make test;

# Package the runner
FROM redis:latest
ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN set -ex;\
    mkdir -p "$LIBDIR";

COPY --from=builder /RSBUILD/docker-build/redisearch.so  "$LIBDIR"
CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redisearch.so"]
