# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from contextlib import contextmanager

from common import *


def config_value(conn, config):
    result = conn.execute_command('CONFIG', 'GET', config)
    return result[config] if isinstance(result, dict) else result[1]


@contextmanager
def all_shards_config(env, config, value):
    conns = env.getOSSMasterNodesConnectionList()
    previous = [config_value(conn, config) for conn in conns]
    try:
        verify_command_OK_on_all_shards(env, 'CONFIG', 'SET', config, value)
        yield
    finally:
        for conn, value in zip(conns, previous):
            conn.execute_command('CONFIG', 'SET', config, value)


@contextmanager
def internal_shard_connections(env):
    """Binary-safe connections to every shard, marked internal so they accept `_FT.*`."""
    pools = []
    try:
        conns = []
        for shard in range(1, env.shardsCount + 1):
            kwargs = dict(env.getConnection(shard).connection_pool.connection_kwargs)
            kwargs['decode_responses'] = False
            pools.append(redis.ConnectionPool(**kwargs))
            conn = redis.Redis(connection_pool=pools[-1])
            conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
            conns.append(conn)
        yield conns
    finally:
        for pool in pools:
            pool.disconnect()


def shard_rows(env, reply):
    """The rows element of a raw `_FT.AGGREGATE` shard reply: one bytes item for a block."""
    return reply[b'results'] if env.protocol == 3 else reply[1:]


def row_block_token(env):
    return '_ROW_BLOCK_RESP3' if env.protocol == 3 else '_ROW_BLOCK'


def assert_keys_on_every_shard(env):
    """Rows must reach the coordinator from every shard, not from a single hash slot's."""
    for conn in env.getOSSMasterNodesConnectionList():
        env.assertGreater(conn.execute_command('DBSIZE'), 0)


def add_docs(env, count, key=lambda i: f'doc:{i}', fields=lambda i: ['n', i]):
    """Hashes keyed without a hash tag by default, so they spread across every shard."""
    conn = getConnectionByEnv(env)
    with conn.pipeline(transaction=False) as pipe:
        for i in range(count):
            pipe.execute_command('HSET', key(i), *fields(i))
        pipe.execute()


def with_optional(i, *fields):
    """`fields`, plus an `optional` field on odd rows only."""
    return [*fields, *(['optional', 'present'] if i % 2 else [])]


def row_block_buffered_reply(env):
    """Shards that buffer every row before replying must still send them as one block."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT', 'optional', 'TEXT').ok()
    count = 300
    add_docs(env, count, fields=lambda i: with_optional(i, 'n', i, 'text', f'value{i}'))
    # Every shard must hold rows for each one's buffered reply to count.
    assert_keys_on_every_shard(env)

    query = ['LOAD', 3, '@n', '@text', '@optional', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, count]
    shard_query = ['_FT.AGGREGATE', 'idx', '*', *query, row_block_token(env)]
    # Both policies make the shard aggregate all rows before replying (startPipelineCommon);
    # with workers that reply is also deferred to the main thread's reply callback.
    for policy in ('fail', 'return-strict'):
        for workers in (0, 2):
            context = dict(policy=policy, workers=workers)
            with all_shards_config(env, ON_TIMEOUT_CONFIG, policy), \
                 all_shards_config(env, 'search-workers', workers):
                with internal_shard_connections(env) as shards:
                    for shard in shards:
                        reply = shard.execute_command(*shard_query)
                        results = shard_rows(env, reply)
                        env.assertEqual(len(results), 1, message=context)
                        env.assertTrue(isinstance(results[0], bytes), message=context)
                        if env.protocol == 3:
                            env.assertGreater(reply[b'row_block_rows'], 0, message=context)


def row_block_no_columns(env):
    """With no column to carry, shards reply RESP rows."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    count = 30
    add_docs(env, count)
    assert_keys_on_every_shard(env)

    with internal_shard_connections(env) as shards:
        for shard in shards:
            reply = shard.execute_command('_FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, count,
                                          row_block_token(env))
            rows = shard_rows(env, reply)
            env.assertEqual(len(rows), shard.execute_command('DBSIZE'))
            env.assertFalse(any(isinstance(row, bytes) for row in rows), message=rows)


@skip(cluster=False)
def test_row_block_buffered_reply_resp3():
    row_block_buffered_reply(Env(protocol=3))


@skip(cluster=False)
def test_row_block_buffered_reply():
    row_block_buffered_reply(Env())


@skip(cluster=False)
def test_row_block_no_columns_resp3():
    row_block_no_columns(Env(protocol=3))


@skip(cluster=False)
def test_row_block_no_columns():
    row_block_no_columns(Env())


@skip(cluster=False)
def test_row_block_resp3_requires_explicit_token():
    """Old coordinators asking for _ROW_BLOCK over RESP3 must still receive legacy rows."""
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', '{rowblock}:1', 'n', 1)
    nonempty = 0
    with internal_shard_connections(env) as shards:
        for shard in shards:
            query = ['_FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n']
            legacy = shard.execute_command(*query)
            env.assertEqual(shard.execute_command(*query, '_ROW_BLOCK'), legacy)
            if legacy[b'results']:
                nonempty += 1
                encoded = shard.execute_command(*query, '_ROW_BLOCK_RESP3')
                env.assertEqual(encoded[b'row_block_rows'], 1)
                env.assertEqual(len(encoded[b'results']), 1)
                env.assertTrue(isinstance(encoded[b'results'][0], bytes))
            else:
                env.assertEqual(legacy[b'results'], [])
    env.assertEqual(nonempty, 1)
