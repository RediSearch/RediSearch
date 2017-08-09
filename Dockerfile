FROM redis:latest

ADD ./src /src
WORKDIR /src
ENV LIBDIR /var/lib/redis/modules
RUN set -ex;\
    deps='python python-setuptools python-pip wget unzip build-essential';\
    apt-get update; \
	apt-get install -y --no-install-recommends $deps;\
    pip install rmtest; \
	make all test;\
    apt-get purge -y $deps;

RUN set -ex;\
    mkdir -p "$LIBDIR";\
    cp redisearch.so "$LIBDIR";\
    rm -rf /src


CMD ["redis-server", "--loadmodule", "/var/lib/redis/modules/redisearch.so"]
