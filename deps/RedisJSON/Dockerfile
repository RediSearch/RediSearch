
#----------------------------------------------------------------------------------------------
FROM redisfab/redis:6.2.7-x64-bullseye AS redis
FROM debian:bullseye-slim AS builder

RUN if [ -f /root/.profile ]; then sed -ie 's/mesg n/tty -s \&\& mesg -n/g' /root/.profile; fi
SHELL ["/bin/bash", "-l", "-c"]

RUN echo "Building for bullseye (debian:bullseye-slim) for x64 [with Redis 6.2.6]"

WORKDIR /build
COPY --from=redis /usr/local/ /usr/local/

ADD . /build

RUN ./deps/readies/bin/getupdates
RUN ./deps/readies/bin/getpy3
RUN ./sbin/system-setup.py

RUN /usr/local/bin/redis-server --version

RUN make build SHOW=1

#----------------------------------------------------------------------------------------------
FROM redisfab/redisearch:master-x64-bullseye AS search
FROM redisfab/redis:6.2.7-x64-bullseye

WORKDIR /data

RUN mkdir -p "/usr/lib/redis/modules"

COPY --from=builder /build/bin/linux-x64-release/rejson.so* "/usr/lib/redis/modules/"
RUN true
COPY --from=search  /usr/lib/redis/modules/redisearch.so* "/usr/lib/redis/modules/"
RUN true

RUN chown -R redis:redis /usr/lib/redis/modules

CMD ["redis-server", \
     "--loadmodule", "/usr/lib/redis/modules/rejson.so", \
     "--loadmodule", "/usr/lib/redis/modules/redisearch.so"]
