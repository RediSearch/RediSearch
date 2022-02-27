#!/bin/bash

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $HERE/.. && pwd)
READIES=$ROOT/deps/readies
. $READIES/shibumi/defs

OP=
[[ $NOP == 1 ]] && OP=echo

$OP mkdir -p $ROOT/bin/tls
$OP cd $ROOT/bin/tls
[[ -f .generated ]] && exit 0

runn openssl genrsa -out ca.key 4096
runn "openssl req \
    -x509 -new -nodes -sha256 \
    -key ca.key \
    -days 3650 \
    -subj '/O=Redis Test/CN=Certificate Authority' \
    -out ca.crt"

PASSOUT=""
PASSIN=""
if [[ $PASSPHRASE != 0 ]]; then
	PASSOUT="-aes256 -passout pass:foobar"
	PASSIN="-passin pass:foobar"
fi

runn openssl genrsa $PASSOUT -out redis.key 2048
runn "openssl req \
    -new -sha256 \
    -key redis.key \
    $PASSIN \
    -subj '/O=Redis Test/CN=Server' | \
    openssl x509 \
        -req -sha256 \
        -CA ca.crt \
        -CAkey ca.key \
        -CAserial ca.txt \
        -CAcreateserial \
        -days 365 \
        -out redis.crt"
runn openssl dhparam -out redis.dh 2048
$OP touch .generated
