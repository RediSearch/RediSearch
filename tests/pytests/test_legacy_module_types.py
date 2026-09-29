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
from test_config import _grep_file_count
from test_short_read import ShardMock

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


RDB_MODULE_OPCODE_FLOAT = 3
RDB_MODULE_OPCODE_DOUBLE = 4
RDB_MODULE_OPCODE_STRING = 5
RDB_OPCODE_SELECTDB = 0xFE
RDB_OPCODE_EOF = 0xFF

# The newest encoding that still routes to the legacy spec loader (LEGACY_INDEX_MAX_VERSION).
LEGACY_SPEC_ENC_VER = 16
# INDEX_MIN_EXPIRE_VERSION: the only encoding that saved the payload of a deleted doc.
LEGACY_SPEC_EXPIRE_ENC_VER = 13
# INDEX_MIN_ALIAS_VERSION: the first encoding that saves the alias list.
LEGACY_SPEC_ALIAS_ENC_VER = 15
DOCUMENT_DELETED = 0x01
DOCUMENT_HAS_PAYLOAD = 0x02
DOCUMENT_HAS_OFFSET_VECTOR = 0x08


def _module_string(value):
    return _save_len(RDB_MODULE_OPCODE_STRING) + _save_len(len(value)) + value


def _module_float(value):
    return _save_len(RDB_MODULE_OPCODE_FLOAT) + struct.pack('<f', value)


def _module_double(value):
    return _save_len(RDB_MODULE_OPCODE_DOUBLE) + struct.pack('<d', value)


def _module_sint(value):
    # Redis saves a signed value as the unsigned integer with the same bits.
    return _module_uint(value & 0xFFFFFFFFFFFFFFFF)


def _byte_offsets(fields, data, data_len=None):
    """Serialize byte offsets the way RSByteOffsets_Serialize does. `data_len` overrides the length
    prefix so the blob can claim more data than it carries."""
    blob = bytes([len(fields)])
    for field_id, first, last in fields:
        blob += bytes([field_id]) + struct.pack('>II', first, last)
    return blob + struct.pack('>I', len(data) if data_len is None else data_len) + data


def _legacy_doc(flags, tail):
    """A doc table record for `doc1` (doc id 1), laid out the same for encver 12 through 16. `tail`
    holds whatever `flags` says follows the fixed fields: payload, sorting vector, byte offsets."""
    return (_module_string(b'doc1')
            + _module_uint(1)            # doc id
            + _module_uint(flags)
            + _module_uint(1)            # max term frequency
            + _module_uint(1)            # doc length
            + _module_float(1.0)         # score
            + tail)


def _legacy_spec_body(doc, encver):
    """An `ft_index0` value for encver 13 through 16: an index `idx` with no fields and the single doc
    record `doc`."""
    return (_module_string(b'idx\0')
            + _module_uint(0)            # index flags
            + _module_uint(0)            # number of fields
            + _module_uint(0) * 10       # index stats
            # Doc table: size (one past the last doc), max doc id, max size.
            + _module_uint(2) + _module_uint(1) + _module_uint(1)
            + doc
            + _module_uint(0)            # terms trie size
            + _module_uint(0)            # timeout
            + (_module_uint(0) if encver >= LEGACY_SPEC_ALIAS_ENC_VER else b''))  # aliases


def _write_legacy_spec_rdb(env, doc, encver=LEGACY_SPEC_ENC_VER):
    """`_write_legacy_spec_body_rdb` for the spec built by `_legacy_spec_body`."""
    return _write_legacy_spec_body_rdb(env, _legacy_spec_body(doc, encver), encver)


def _write_legacy_spec_body_rdb(env, body, encver):
    """Stop the server and replace its RDB file with one holding a single legacy index spec, the
    encver-`encver` `ft_index0` value `body`. A legacy spec is only upgraded during the first RDB load
    after the module loads, so it cannot be RESTOREd (see testLegacyIndexSpecRestoreIsRefused) and has
    to come from the file the server starts from. Returns the server's log file path."""
    conn = _binary_conn(env)
    rdb_version = _rdb_version(conn)
    db_dir = env.cmd('CONFIG', 'GET', 'dir')[1]
    rdb_path = os.path.join(db_dir, env.cmd('CONFIG', 'GET', 'dbfilename')[1])
    log_path = os.path.join(db_dir, env.cmd('CONFIG', 'GET', 'logfile')[1])
    env.stop()

    # INDEX_SPEC_KEY_FMT for the spec named `idx`: the key the upgrade deletes once it has loaded it.
    key = b'idx:idx'
    rdb = (b'REDIS%04d' % rdb_version
           + bytes([RDB_OPCODE_SELECTDB]) + _save_len(0)
           + bytes([RDB_TYPE_MODULE_2]) + _save_len(len(key)) + key
           + _save_len(_module_type_id('ft_index0', encver))
           + body
           + _save_len(RDB_MODULE_OPCODE_EOF)
           + bytes([RDB_OPCODE_EOF]))
    with open(rdb_path, 'wb') as f:
        f.write(rdb + struct.pack('<Q', _crc64(rdb)))
    return log_path


@skip(cluster=True, asan=True)
def testLegacySpecWithByteOffsetsLoads():
    """The fixture itself, and byte offsets that fill their blob exactly, load and upgrade."""
    env = Env(moduleArgs='UPGRADE_INDEX idx; PREFIX 1 doc')
    skipOnExistingEnv(env)
    offsets = _byte_offsets([(0, 1, 1)], b'\x05')
    _write_legacy_spec_rdb(env, _legacy_doc(DOCUMENT_HAS_OFFSET_VECTOR, _module_string(offsets)))
    env.start()
    env.assertEqual(index_info(env, 'idx')['index_name'], 'idx')


@skip(cluster=True, asan=True)
def testLegacySpecWithTruncatedByteOffsetsFailsToLoad():
    """Byte offsets whose length prefix claims more data than the blob holds must fail the load
    cleanly. The parser used to trust the prefix: it allocated the claimed length and copied it from
    the end of the blob, crashing the server."""
    env = Env(moduleArgs='UPGRADE_INDEX idx; PREFIX 1 doc')
    skipOnExistingEnv(env)
    offsets = _byte_offsets([], b'', data_len=0x7FFFFFFF)
    log_path = _write_legacy_spec_rdb(
        env, _legacy_doc(DOCUMENT_HAS_OFFSET_VECTOR, _module_string(offsets)))

    # Give the server time to fail during the load before RLTest's readiness probe races with it.
    env.envRunner.startupGraceSecs = 1
    try:
        env.start()
    except Exception as e:
        env.assertContains('Redis server is dead', str(e))
    env.assertFalse(env.isUp())

    with open(log_path) as f:
        log = f.read()
    # Redis writes a bug report for both a signal and a failed assertion.
    env.assertNotContains('REDIS BUG REPORT', log, message=log[-4000:])
    env.assertContains('truncated byte offsets for doc id 1', log)


@skip(cluster=True)
def testLegacySpecWithDeletedPayloadDocLoads():
    """A deleted doc flagged as having a payload is dropped during the load without its payload ever
    being read. Its flag used to survive, so freeing the dropped doc dereferenced a NULL payload and
    crashed the server mid-load."""
    env = Env(moduleArgs='UPGRADE_INDEX idx; PREFIX 1 doc')
    skipOnExistingEnv(env)
    _write_legacy_spec_rdb(env, _legacy_doc(DOCUMENT_DELETED | DOCUMENT_HAS_PAYLOAD, b''))
    env.start()
    env.assertTrue(env.isUp())
    info = index_info(env, 'idx')
    env.assertEqual(info['index_name'], 'idx')
    env.assertEqual(info['num_docs'], 0)


@skip(cluster=True)
def testLegacySpecWithDeletedPayloadDocLoadsExpireEncoding():
    """Encver 13 saved the payload of a deleted doc anyway. The load must skip that string as well as
    clear the flag: if it left the string unread, every later field would be read out of place and the
    load would fail."""
    env = Env(moduleArgs='UPGRADE_INDEX idx; PREFIX 1 doc')
    skipOnExistingEnv(env)
    doc = _legacy_doc(DOCUMENT_DELETED | DOCUMENT_HAS_PAYLOAD, _module_string(b'payload\0'))
    _write_legacy_spec_rdb(env, doc, encver=LEGACY_SPEC_EXPIRE_ENC_VER)
    env.start()
    env.assertTrue(env.isUp())
    info = index_info(env, 'idx')
    env.assertEqual(info['index_name'], 'idx')
    env.assertEqual(info['num_docs'], 0)


# The newest encoding whose fields have no separate path (INDEX_MIN_TAGFIELD_VERSION - 1).
LEGACY_SPEC_NO_FIELD_PATH_ENC_VER = 7
INDEXFLD_T_FULLTEXT = 0x01


def _legacy_spec_body_no_field_path():
    """An encver-7 `ft_index0` value: an index `idx` with one TEXT field `t` and no documents. Fields
    of this encoding have a name but no path."""
    return (_module_string(b'idx\0')
            + _module_uint(0)            # index flags
            + _module_uint(1)            # number of fields
            + _module_string(b't\0')     # field name
            + _module_uint(0)            # field id
            + _module_uint(INDEXFLD_T_FULLTEXT)
            + _module_double(1.0)        # weight
            + _module_uint(0)            # field options
            + _module_sint(-1)           # sort index: not sortable
            + _module_uint(0) * 10       # index stats
            # Doc table: size (one past the last doc) and max doc id; this encoding has no max size.
            + _module_uint(1) + _module_uint(0)
            + _module_uint(0))           # terms trie size


@skip(cluster=True)
def testLegacySpecWithoutFieldPathLoads():
    """A field from an encoding that predates field paths loads with its name as its path, so the
    upgraded index serves the field. The path used to stay NULL, which crashed the server while the
    load built the spec's field cache."""
    env = Env(moduleArgs='UPGRADE_INDEX idx; PREFIX 1 doc')
    skipOnExistingEnv(env)
    _write_legacy_spec_body_rdb(env, _legacy_spec_body_no_field_path(),
                                LEGACY_SPEC_NO_FIELD_PATH_ENC_VER)
    env.start()
    env.assertTrue(env.isUp())
    info = index_info(env, 'idx')
    env.assertEqual(info['index_name'], 'idx')
    attribute = to_dict(info['attributes'][0])
    env.assertEqual(attribute['identifier'], 't')
    env.assertEqual(attribute['attribute'], 't')

    env.expect('HSET', 'doc:1', 't', 'hello').equal(1)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc:1'])

    # Saving the upgraded index writes it in the current format, which records the field's path.
    env.dumpAndReload()
    waitForIndex(env, 'idx')
    attribute = to_dict(index_info(env, 'idx')['attributes'][0])
    env.assertEqual(attribute['identifier'], 't')
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([1, 'doc:1'])


def _fail_full_sync(env, shard_mock):
    """Make the server a replica of `shard_mock` and cut its full sync short, so the load fails.

    The load has to be diskless: a truncated RDB loaded from disk makes Redis exit instead of firing
    `LOADING_FAILED`. `on-empty-db` needs an empty keyspace, but unlike `swapdb` it does not require
    every module to support async loading."""
    env.cmd('CONFIG', 'SET', 'repl-diskless-load', 'on-empty-db')
    env.cmd('REPLICAOF', '127.0.0.1', shard_mock.server_port)
    conn = shard_mock.GetConnection(timeout=10)
    env.assertEqual(conn.read_request(), ['PING'])
    conn.send_status('PONG')
    req = conn.read_request()
    while req[0] == 'REPLCONF':
        conn.send_status('OK')
        req = conn.read_request()
    env.assertEqual(req[0], 'PSYNC')
    conn.send_status('FULLRESYNC af4e30b5d14dce9f96fbb7769d0ec794cdc0bbcc 0')
    # Announce more bytes than we send, then hang up mid-header.
    conn.send(b'$1000\r\nREDIS')
    conn.flush()
    conn.close()

    for _ in range(100):
        try:
            env.cmd('PING')
            break
        except redis.exceptions.BusyLoadingError:
            time.sleep(0.1)
    env.cmd('REPLICAOF', 'NO', 'ONE')


@skip(cluster=True, redis_less_than='7.0.0')
def testLegacyIndexSpecRestoreIsRefusedAfterFailedLoad(env):
    """A failed load leaves the legacy-spec registry and the UPGRADE_INDEX rules allocated, so the
    refusal must key on loading state, not on the globals. The rules are freed only when a load
    succeeds, so this needs a server that has not completed one since startup - hence the restart
    without an RDB. A replica whose first full sync failed and was then promoted is in that state."""
    skipOnExistingEnv(env)
    if env.useSlaves or env.useAof:
        env.skip()

    dbDir = env.cmd('CONFIG', 'GET', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, env.cmd('CONFIG', 'GET', 'dbfilename')[1])
    logFilePath = os.path.join(dbDir, env.cmd('CONFIG', 'GET', 'logfile')[1])
    env.stop()
    if os.path.exists(rdbFilePath):
        os.unlink(rdbFilePath)
    env.start()

    failedSyncMsg = 'Failed trying to load the MASTER synchronization DB'
    failedSyncsBefore = _grep_file_count(logFilePath, failedSyncMsg)
    with ShardMock(env) as shardMock:
        _fail_full_sync(env, shardMock)
    env.assertGreater(_grep_file_count(logFilePath, failedSyncMsg), failedSyncsBefore,
                      message='the full sync did not fail, so this test proves nothing')

    conn = _binary_conn(env)
    # The name is stored as a string; a uint in its place makes the read fail, which crashed the
    # server when the loader got that far.
    payload = _dump_payload(conn, 'ft_index0', _module_uint(0), encver=16)
    env.expect('RESTORE', 'idx:legacy', 0, payload).error().contains('Bad data format')
    env.assertTrue(env.isUp())
