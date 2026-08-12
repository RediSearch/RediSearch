# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import struct

import redis

from includes import *
from common import *
from RLTest import Env

LEGACY_ENC_VER = 1

RDB_TYPE_MODULE_2 = 7
RDB_MODULE_OPCODE_EOF = 0
RDB_MODULE_OPCODE_UINT = 2

RDB_6BITLEN = 0
RDB_14BITLEN = 1
RDB_32BITLEN = 0x80
RDB_64BITLEN = 0x81

# Redis's module type ids pack 9 six-bit characters plus a 10-bit encoding version.
_MODULE_TYPE_CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'


def _binary_conn(env, db=None):
    """A connection to the same server that does not decode replies.

    `DUMP` returns arbitrary bytes, which the default RLTest client tries to decode as UTF-8. Rebuild
    the client from the source pool's own connection class and kwargs rather than calling
    `redis.Redis(**kwargs)`: the class is what carries the TLS and unix-socket selectors, and
    `connection_kwargs` alone loses them - it has no `ssl` flag, and its `path` is spelled
    `unix_socket_path` on the `redis.Redis` constructor.
    """
    pool = env.getConnection().connection_pool
    kwargs = dict(pool.connection_kwargs)
    kwargs['decode_responses'] = False
    if db is not None:
        kwargs['db'] = db
    return redis.Redis(connection_pool=redis.ConnectionPool(
        connection_class=pool.connection_class, **kwargs))


def _crc64(data):
    # Reflected form of the Jones polynomial Redis uses, init 0, no final xor.
    poly = 0x95AC9329AC4BC9B5
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ poly if crc & 1 else crc >> 1
    return crc


def _save_len(n):
    if n < (1 << 6):
        return bytes([(RDB_6BITLEN << 6) | n])
    if n < (1 << 14):
        return bytes([(RDB_14BITLEN << 6) | (n >> 8), n & 0xFF])
    if n <= 0xFFFFFFFF:
        return bytes([RDB_32BITLEN]) + struct.pack('>I', n)
    return bytes([RDB_64BITLEN]) + struct.pack('>Q', n)


def _module_type_id(name, encver):
    assert len(name) == 9, 'module type names are exactly 9 characters'
    packed = 0
    for ch in name:
        packed = (packed << 6) | _MODULE_TYPE_CHARSET.index(ch)
    return (packed << 10) | encver


def _module_uint(value):
    return _save_len(RDB_MODULE_OPCODE_UINT) + _save_len(value)


def _rdb_version(conn):
    # Take the version from a payload the server produced, so we never guess a value it would reject.
    conn.execute_command('SET', '_probe', 'x')
    dumped = conn.execute_command('DUMP', '_probe')
    conn.execute_command('DEL', '_probe')
    return struct.unpack('<H', dumped[-10:-8])[0]


def _dump_payload(conn, type_name, body, encver=LEGACY_ENC_VER):
    """Build a RESTORE-able payload for a module value of `type_name` whose body is `body`."""
    obj = (bytes([RDB_TYPE_MODULE_2])
           + _save_len(_module_type_id(type_name, encver))
           + body
           + _save_len(RDB_MODULE_OPCODE_EOF))
    blob = obj + struct.pack('<H', _rdb_version(conn))
    return blob + struct.pack('<Q', _crc64(blob))


@skip(cluster=True)
def testLegacyIndexSpecRestoreIsRefused(env):
    """A legacy index *spec* key (`ft_index0`) can only be upgraded during an RDB load: the
    `UPGRADE_INDEX` rules and the legacy-spec registry are built for the duration of a load and released
    at the end of it. Restoring one on a running server used to reach `dictFetchValue` on those NULL
    globals and segfault, so this test failing looks like a dead server rather than an assertion.
    MOD-15685 finding #71."""
    skipOnExistingEnv(env)
    conn = _binary_conn(env)

    # Any body will do: the guard refuses before reading a byte. LEGACY_INDEX_MAX_VERSION is 16, and
    # anything in 2..16 routes to the legacy spec loader.
    for encver in (2, 9, 16):
        key = 'idx:legacy:{}'.format(encver)
        payload = _dump_payload(conn, 'ft_index0', _module_uint(0), encver=encver)
        # Assert on the message, not merely that something was raised: `Query` catches every
        # exception, so a bare `.error()` would also be satisfied by the `ConnectionError` of the
        # crash this test exists to catch - and by the 'payload version or checksum are wrong' of a
        # drifted helper.
        env.expect('RESTORE', key, 0, payload).error().contains('Bad data format')
        env.assertEqual(conn.execute_command('EXISTS', key), 0, message=key)

    env.assertTrue(env.isUp())
