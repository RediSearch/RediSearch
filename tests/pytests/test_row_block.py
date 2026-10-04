# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from contextlib import contextmanager

from common import *


@contextmanager
def row_block_format(env, enabled):
    conn = env.getConnection()
    config = 'search-internal-row-block-format'
    result = conn.execute_command('CONFIG', 'GET', config)
    previous = result[config] if env.protocol == 3 else result[1]
    try:
        env.assertEqual(conn.execute_command('CONFIG', 'SET', config, enabled), 'OK')
        yield
    finally:
        conn.execute_command('CONFIG', 'SET', config, previous)


def row_block_rows(env, reply):
    if env.protocol == 3:
        env.assertEqual(set(reply), {'attributes', 'warning', 'total_results', 'format', 'results'})
        env.assertEqual(reply['warning'], [])
        env.assertEqual(reply['attributes'], [])
        env.assertEqual(reply['format'], 'STRING')
        for row in reply['results']:
            env.assertEqual(set(row), {'extra_attributes', 'values'})
            env.assertEqual(row['values'], [])
        return [row['extra_attributes'] for row in reply['results']]
    return [dict(zip(row[::2], row[1::2])) for row in reply[1:]]


@contextmanager
def all_shards_config(env, config, value):
    conns = env.getOSSMasterNodesConnectionList()
    previous = []
    for conn in conns:
        result = conn.execute_command('CONFIG', 'GET', config)
        previous.append(result[config] if isinstance(result, dict) else result[1])
    try:
        verify_command_OK_on_all_shards(env, 'CONFIG', 'SET', config, value)
        yield
    finally:
        for conn, value in zip(conns, previous):
            conn.execute_command('CONFIG', 'SET', config, value)


def coordinator_network_profile(env, profile):
    if env.protocol == 3:
        coordinator = profile['Profile']['Coordinator']
        return coordinator['Result processors profile'][0]
    coordinator = to_dict(to_dict(profile[1])['Coordinator'])
    return to_dict(coordinator['Result processors profile'][0])


def row_block_cursor_values(env):
    """Decode strings, numbers and missing fields across shard and client cursors."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT', 'optional', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    # Exceed the shard's default chunk size; client COUNT alone does not do so.
    count = 1005
    with conn.pipeline(transaction=False) as pipe:
        for i in range(count):
            fields = ['n', i, 'text', f'value\x00{i}']
            if i % 2:
                fields += ['optional', 'present']
            pipe.execute_command('HSET', f'{{rowblock}}:{i}', *fields)
        pipe.execute()

    expected = [dict(n=str(i), text=f'value\x00{i}',
                     **({'optional': 'present'} if i % 2 else {})) for i in range(count)]
    for enabled in ('no', 'yes'):
        with row_block_format(env, enabled):
            reply, cursor = env.cmd(
                'FT.AGGREGATE', 'idx', '*', 'LOAD', 3, '@n', '@text', '@optional',
                'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, count, 'WITHCURSOR', 'COUNT', 127)
            rows = row_block_rows(env, reply)
            env.assertNotEqual(cursor, 0, message=reply)
            while cursor:
                reply, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 127)
                rows.extend(row_block_rows(env, reply))
            env.assertEqual(rows, expected)
            network = coordinator_network_profile(env, env.cmd(
                'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',
                'LOAD', 3, '@n', '@text', '@optional', 'LIMIT', 0, count))
            # Binary rows include a null slot for a missing field; RESP rows omit it.
            # This distinguishes actual binary decoding from silently using RESP.
            fields = 3 * count if enabled == 'yes' else 2 * count + count // 2
            env.assertEqual(network['Fields converted'], fields, message=network)
            env.assertGreaterEqual(network['Shard replies'], 2, message=network)


def row_block_reducer_arrays(env):
    """Shard TOLIST arrays and numeric partial sums survive binary transport."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'category', 'TAG', 'amount', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i in range(12):
        conn.execute_command('HSET', f'{{rowblock}}:{i}', 'category', str(i % 3),
                             'amount', i, 'label', f'label{i}')
    expected = [dict(category=str(group), total=str(sum(range(group, 12, 3))),
                     labels=sorted(f'label{i}' for i in range(group, 12, 3)))
                for group in range(3)]
    for enabled in ('no', 'yes'):
        with row_block_format(env, enabled):
            reply = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@label',
                            'GROUPBY', 1, '@category',
                            'REDUCE', 'SUM', 1, '@amount', 'AS', 'total',
                            'REDUCE', 'TOLIST', 1, '@label', 'AS', 'labels',
                            'SORTBY', 2, '@category', 'ASC')
            rows = row_block_rows(env, reply)
            for row in rows:
                row['labels'].sort()
            env.assertEqual(rows, expected)


def row_block_dynamic_schema_fallback(env):
    """A changing LOAD * schema replays earlier encoded rows without data loss."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'common', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(6):
        conn.execute_command('HSET', f'{{rowblock}}:{i}', 'common', 'x', f'field{i}', i)
    expected = sorted(sorted([('common', 'x'), (f'field{i}', str(i))]) for i in range(6))
    # FAIL loads every row before encoding any, so its block carries the grown schema with
    # null slots instead of falling back; both must yield the same rows.
    for policy in ('return', 'fail'):
        with all_shards_config(env, ON_TIMEOUT_CONFIG, policy):
            for enabled in ('no', 'yes'):
                with row_block_format(env, enabled):
                    reply = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '*')
                    rows = sorted(sorted(row.items()) for row in row_block_rows(env, reply))
                    env.assertEqual(rows, expected, message=policy)


def row_block_buffered_reply(env):
    """Shards that buffer every row before replying must still send them as one block."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT', 'optional', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    count = 300
    with conn.pipeline(transaction=False) as pipe:
        for i in range(count):
            fields = ['n', i, 'text', f'value{i}']
            if i % 2:
                fields += ['optional', 'present']
            # No hashtag: every shard must hold rows for each one's buffered reply to count.
            pipe.execute_command('HSET', f'doc:{i}', *fields)
        pipe.execute()

    query = ['LOAD', 3, '@n', '@text', '@optional', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, count]
    expected = [dict(n=str(i), text=f'value{i}',
                     **({'optional': 'present'} if i % 2 else {})) for i in range(count)]
    shard_query = ['_FT.AGGREGATE', 'idx', '*', *query,
                   '_ROW_BLOCK_RESP3' if env.protocol == 3 else '_ROW_BLOCK']
    # Both policies make the shard aggregate all rows before replying (startPipelineCommon);
    # with workers that reply is also deferred to the main thread's reply callback.
    for policy in ('fail', 'return-strict'):
        for workers in (0, 2):
            context = dict(policy=policy, workers=workers)
            with all_shards_config(env, ON_TIMEOUT_CONFIG, policy), \
                 all_shards_config(env, 'search-workers', workers):
                for enabled in ('no', 'yes'):
                    with row_block_format(env, enabled):
                        reply = env.cmd('FT.AGGREGATE', 'idx', '*', *query)
                        env.assertEqual(row_block_rows(env, reply), expected, message=context)
                        network = coordinator_network_profile(env, env.cmd(
                            'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', *query))
                        # A block carries a null slot for each missing field; RESP rows omit it.
                        fields = 3 * count if enabled == 'yes' else 2 * count + count // 2
                        env.assertEqual(network['Fields converted'], fields,
                                        message=(context, network))

                for shard in range(1, env.shardsCount + 1):
                    kwargs = dict(env.getConnection(shard).connection_pool.connection_kwargs)
                    kwargs['decode_responses'] = False
                    pool = redis.ConnectionPool(**kwargs)
                    try:
                        direct = redis.Redis(connection_pool=pool)
                        direct.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
                        reply = direct.execute_command(*shard_query)
                        results = reply[b'results'] if env.protocol == 3 else reply[1:]
                        env.assertEqual(len(results), 1, message=context)
                        env.assertTrue(isinstance(results[0], bytes), message=context)
                        if env.protocol == 3:
                            env.assertGreater(reply[b'row_block_rows'], 0, message=context)
                    finally:
                        pool.disconnect()


@skip(cluster=False)
def test_row_block_buffered_reply_resp3():
    """RESP3 buffered replies carry a block and its row count."""
    row_block_buffered_reply(Env(protocol=3))


@skip(cluster=False)
def test_row_block_buffered_reply(env):
    """RESP2 buffered replies carry a block."""
    row_block_buffered_reply(env)


@skip(cluster=False)
def test_row_block_cursor_values_resp3():
    """RESP3 keeps public cursor/profile wrappers while shard rows use blocks."""
    row_block_cursor_values(Env(protocol=3))


@skip(cluster=False)
def test_row_block_reducer_arrays_resp3():
    """RESP3 preserves reducer arrays and numeric partial sums."""
    row_block_reducer_arrays(Env(protocol=3))


@skip(cluster=False)
def test_row_block_dynamic_schema_fallback_resp3():
    """RESP3 replay must wrap previously encoded rows just like ordinary rows."""
    row_block_dynamic_schema_fallback(Env(protocol=3))


@skip(cluster=False)
def test_row_block_resp3_counts():
    """A binary chunk counts its rows, even when LIMIT consumes only part of it."""
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(20):
        conn.execute_command('HSET', f'{{rowblock}}:{i}', 'n', i)
    for tail in (['LIMIT', 0, 1], ['LIMIT', 0, 0],
                 ['FILTER', '@n > 100', 'LIMIT', 0, 10],
                 ['SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, 1, 'WITHCOUNT']):
        query = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n', *tail]
        if 'WITHCOUNT' in query:
            query.remove('WITHCOUNT')
            query.insert(3, 'WITHCOUNT')
        with row_block_format(env, 'no'):
            expected = env.cmd(*query)
        with row_block_format(env, 'yes'):
            actual = env.cmd(*query)
        env.assertEqual(actual, expected)
        row_block_rows(env, actual)


@skip(cluster=False)
def test_row_block_resp3_requires_explicit_token():
    """Old coordinators asking for _ROW_BLOCK over RESP3 must still receive legacy rows."""
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', '{rowblock}:1', 'n', 1)
    nonempty = 0
    for shard in range(1, env.shardsCount + 1):
        kwargs = dict(env.getConnection(shard).connection_pool.connection_kwargs)
        kwargs['decode_responses'] = False
        pool = redis.ConnectionPool(**kwargs)
        try:
            direct = redis.Redis(connection_pool=pool)
            direct.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
            query = ['_FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n']
            legacy = direct.execute_command(*query)
            env.assertEqual(direct.execute_command(*query, '_ROW_BLOCK'), legacy)
            if legacy[b'results']:
                nonempty += 1
                encoded = direct.execute_command(*query, '_ROW_BLOCK_RESP3')
                env.assertEqual(encoded[b'row_block_rows'], 1)
                env.assertEqual(len(encoded[b'results']), 1)
                env.assertTrue(isinstance(encoded[b'results'][0], bytes))
            else:
                env.assertEqual(legacy[b'results'], [])
        finally:
            pool.disconnect()
    env.assertEqual(nonempty, 1)


@skip(cluster=False)
def test_row_block_cursor_values(env):
    """RESP2 preserves values across shard/client cursors and profile replies."""
    row_block_cursor_values(env)


@skip(cluster=False)
def test_row_block_reducer_arrays(env):
    """RESP2 preserves reducer arrays and numeric partial sums."""
    row_block_reducer_arrays(env)


@skip(cluster=False)
def test_row_block_dynamic_schema_fallback(env):
    """RESP2 replays earlier encoded rows when LOAD * changes the schema."""
    row_block_dynamic_schema_fallback(env)
