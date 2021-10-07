#!/bin/bash

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd "$HERE" && pwd)

mkdir -p $ROOT/tests/tls
cd $ROOT/tests/tls

openssl genrsa -out ca.key 4096
openssl req \
    -x509 -new -nodes -sha256 \
    -key ca.key \
    -days 3650 \
    -subj '/O=Redis Test/CN=Certificate Authority' \
    -out ca.crt
openssl genrsa -out redis.key 2048
openssl req \
    -new -sha256 \
    -key redis.key \
    -subj '/O=Redis Test/CN=Server' | \
    openssl x509 \
        -req -sha256 \
        -CA ca.crt \
        -CAkey ca.key \
        -CAserial ca.txt \
        -CAcreateserial \
        -days 365 \
        -out redis.crt
openssl dhparam -out redis.dh 2048
