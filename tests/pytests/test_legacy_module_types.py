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

# End-to-end coverage for the pre-2.0 module types (ft_invidx / numericdx / ft_tagidx). These keys
# can only be created by deserializing an old payload, so RESTORE is the only way to get one into a
# live server - which is also how they reach production, since Redis Enterprise import forwards
# RESTORE commands rather than handing an RDB to Redis.
#
# The C++ tests in test_cpp_rdb.cpp call the callbacks directly against a mock whose framing is not
# Redis's, so they cannot prove the bytes we emit are valid Redis framing. These tests cross that
# boundary: Redis itself validates the module type id, the module EOF marker, the DUMP footer and
# the CRC. See MOD-15685.

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
    """A connection that does not decode replies. DUMP returns arbitrary bytes, which the default
    RLTest client tries to decode as UTF-8."""
    kwargs = dict(env.getConnection().connection_pool.connection_kwargs)
    kwargs['decode_responses'] = False
    if db is not None:
        kwargs['db'] = db
    # A pool built for a unix socket reports it as `path`, but the client constructor takes
    # `unix_socket_path`. Forwarding the pool's kwargs verbatim breaks under UNIX=1.
    if 'path' in kwargs:
        kwargs['unix_socket_path'] = kwargs.pop('path')
    return redis.Redis(**kwargs)


def _crc64(data):
    # Reflected form of the Jones polynomial Redis uses, init 0, no final xor.
    # testDumpPayloadHelperMatchesRedis proves this matches the server rather than assuming it.
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


# The minimal bodies the fix emits, expressed independently of the C code so that a change to either
# side has to be reflected here deliberately.
def _legacy_bodies():
    return {
        'ft_invidx': _module_uint(0) * 4,  # flags, lastId, numDocs, n_blocks
        'numericdx': _module_uint(0),      # v1 terminator, also a zero count under v0
        'ft_tagidx': _module_uint(0),      # n_tags
    }


@skip(cluster=True)
def testDumpPayloadHelperMatchesRedis(env):
    """Guard the helper itself. If the CRC or length encoding were wrong, every other test here would
    fail with 'Bad data format' - a confusing way to learn the fixture is broken rather than the code."""
    skipOnExistingEnv(env)
    conn = _binary_conn(env)

    conn.execute_command('SET', 'plain', 'hello')
    dumped = conn.execute_command('DUMP', 'plain')

    body, footer = dumped[:-8], dumped[-8:]
    env.assertEqual(struct.unpack('<Q', footer)[0], _crc64(body),
                    message='our crc64 does not match the one Redis wrote')

    conn.execute_command('RESTORE', 'plain_copy', 0, dumped)
    env.assertEqual(conn.execute_command('GET', 'plain_copy'), b'hello')


@skip(cluster=True)
def testLegacyEmptyPayloadRoundTrips(env):
    """A legacy key must survive RESTORE -> DUMP -> RESTORE and a real reload. Before the fix the save
    side wrote zero bytes, so the reload failed with 'not terminated by the proper module value EOF
    marker' and took the server down."""
    skipOnExistingEnv(env)
    conn = _binary_conn(env)

    for type_name, body in _legacy_bodies().items():
        key = 'legacy:' + type_name
        conn.execute_command('RESTORE', key, 0, _dump_payload(conn, type_name, body))
        env.assertEqual(conn.execute_command('TYPE', key), type_name.encode(), message=key)

        # DUMP exercises the new rdb_save; restoring the result runs the loader over our own bytes.
        redumped = conn.execute_command('DUMP', key)
        conn.execute_command('RESTORE', key + ':copy', 0, redumped)
        env.assertEqual(conn.execute_command('TYPE', key + ':copy'), type_name.encode(), message=key)

    expected = conn.execute_command('DBSIZE')

    # The whole point of the fix: reloading a dataset that contains these keys must succeed.
    env.dumpAndReload()
    conn = _binary_conn(env)
    env.assertEqual(conn.execute_command('DBSIZE'), expected)
    for type_name in _legacy_bodies():
        env.assertEqual(conn.execute_command('TYPE', 'legacy:' + type_name), type_name.encode())
