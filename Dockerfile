FROM redis:latest as builder

ADD ./src /src
WORKDIR /src
ENV LIBDIR /var/lib/redis/modules
ENV DEPS "python python-setuptools python-pip wget unzip build-essential"
RUN set -ex;\
    deps="$DEPS";\
    apt-get update; \
	apt-get install -y --no-install-recommends $deps;\
    pip install rmtest; 
RUN set -ex;\
    deps="$DEPS";\
    make all test;\
    apt-get purge -y $deps;

FROM redis:latest
ENV LIBDIR /var/lib/redis/modules
WORKDIR /data
RUN set -ex;\
    mkdir -p "$LIBDIR";
COPY --from=builder /src/redisearch.so  "$LIBDIR"

CMD ["redis-server", "--loadmodule", "/var/lib/redis/modules/redisearch.so"]
