# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import struct
import time

import redis

from includes import *
from common import *
from RLTest import Env

# Coverage for the cleanup of orphaned pre-2.0 index keys (ft_invidx / numericdx / ft_tagidx).
#
# Such a key can only be created by deserializing an old payload, so RESTORE is the only way to get
# one into a live server - which is also how they reach production, since Redis Enterprise import
# forwards RESTORE commands rather than handing an RDB to Redis, and therefore fires no loading event
# for any load-time sweep to hang off. See MOD-15685.
#
# The payload builder is duplicated from test_legacy_module_types.py on the serialization-fix branch;
# fold the two together once that lands.

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


def _as_text(value):
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _waitForAofRewrite(env, conn, timeout=60):
    """`waitForRdbSaveToFinish` only polls rdb_bgsave_in_progress, so it returns immediately here and
    DEBUG LOADAOF would race a rewrite that is still running or merely scheduled."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        # redis-py applies its own INFO response callback, so this comes back already parsed into a
        # dict; the raw-text branch is only for a client that has that callback disabled.
        info = conn.execute_command('INFO', 'persistence')
        if isinstance(info, dict):
            fields = {_as_text(k): _as_text(v) for k, v in info.items()}
        else:
            fields = dict(line.split(':', 1)
                          for line in _as_text(info).splitlines() if ':' in line)
        if (fields.get('aof_rewrite_in_progress', '0').strip() == '0'
                and fields.get('aof_rewrite_scheduled', '0').strip() == '0'):
            return
        time.sleep(0.1)
    env.assertTrue(False, message='AOF rewrite did not finish within {}s'.format(timeout))


def _expect_restore_error(env, conn, key, payload, message):
    try:
        conn.execute_command('RESTORE', key, 0, payload)
        env.assertTrue(False, message='expected RESTORE to be rejected: ' + message)
    except redis.exceptions.ResponseError:
        pass
    env.assertEqual(conn.execute_command('EXISTS', key), 0, message=message)


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
    """Guard the builder. A wrong CRC or length encoding would make every test below fail with
    'Bad data format', which is a confusing way to learn the fixture is broken rather than the code."""
    skipOnExistingEnv(env)
    conn = _binary_conn(env)

    conn.execute_command('SET', 'plain', 'hello')
    dumped = conn.execute_command('DUMP', 'plain')
    env.assertEqual(struct.unpack('<Q', dumped[-8:])[0], _crc64(dumped[:-8]),
                    message='our crc64 does not match the one Redis wrote')


@skip(cluster=True)
def testRestoredLegacyKeyIsRemoved(env):
    """A legacy key restored onto a writable primary must not survive the command. This is the path
    that produced ~101k orphans in production, where nothing ever cleaned them up."""
    skipOnExistingEnv(env)
    conn = _binary_conn(env)

    for type_name, body in _legacy_bodies().items():
        key = 'legacy:' + type_name
        conn.execute_command('RESTORE', key, 0, _dump_payload(conn, type_name, body))
        # The post-notification job runs before the next command is served, so it is already gone.
        env.assertEqual(conn.execute_command('EXISTS', key), 0, message=key)

    env.assertEqual(conn.execute_command('DBSIZE'), 0)
    env.assertTrue(env.isUp())


@skip(cluster=True)
def testCleanupDoesNotDeleteAReplacementValue(env):
    """The delete is deferred to a post-notification job, which runs after the whole execution unit.
    Inside MULTI the legacy value can be replaced before the job runs, so the job must re-check the
    value rather than trusting the key name it was queued with."""
    skipOnExistingEnv(env)
    conn = _binary_conn(env)

    key = 'replaced'
    payload = _dump_payload(conn, 'ft_invidx', _legacy_bodies()['ft_invidx'])

    pipe = conn.pipeline(transaction=True)
    pipe.execute_command('RESTORE', key, 0, payload)
    pipe.execute_command('SET', key, 'valuable')
    pipe.execute()

    env.assertEqual(conn.execute_command('GET', key), b'valuable',
                    message='cleanup deleted a value written after the legacy key')
    env.assertTrue(env.isUp())


@skip(cluster=True)
def testCleanupLeavesOrdinaryKeysAlone(env):
    """The match is on module type plus sentinel, not on key naming, so ordinary keys - including ones
    named like a legacy index key - must be untouched."""
    skipOnExistingEnv(env)
    conn = _binary_conn(env)

    conn.execute_command('SET', 'ft:idx/term', 'not-a-legacy-key')
    conn.execute_command('HSET', 'doc:1', 'title', 'hello')

    dumped = conn.execute_command('DUMP', 'ft:idx/term')
    conn.execute_command('RESTORE', 'ft:idx/term:copy', 0, dumped)

    env.assertEqual(conn.execute_command('GET', 'ft:idx/term'), b'not-a-legacy-key')
    env.assertEqual(conn.execute_command('GET', 'ft:idx/term:copy'), b'not-a-legacy-key')
    env.assertEqual(conn.execute_command('EXISTS', 'doc:1'), 1)
    env.assertTrue(env.isUp())
