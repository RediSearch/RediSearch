# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import os
import tempfile
from contextlib import contextmanager

from common import *

ROW_BLOCK_CONFIG = 'search-internal-row-block-format'


def config_value(conn, config=ROW_BLOCK_CONFIG):
    result = conn.execute_command('CONFIG', 'GET', config)
    return result[config] if isinstance(result, dict) else result[1]


def row_block_env(**kwargs):
    """An Env whose servers start with row blocks on, set in the config file (the flag has no module argument)."""
    path = os.path.join(tempfile.gettempdir(), f'rs_test_row_block_{os.getpid()}.conf')
    with open(path, 'w') as f:
        if Defaults.redis_config_file:
            # An explicit config file replaces the suite-wide one rather than adding to it.
            f.write(f'include {os.path.abspath(Defaults.redis_config_file)}\n')
        f.write(f'{ROW_BLOCK_CONFIG} yes\n')
    try:
        env = Env(redisConfigFile=path, **kwargs)
    finally:
        # Servers read it at startup only.
        os.unlink(path)
    for conn in env.getOSSMasterNodesConnectionList():
        env.assertEqual(config_value(conn), 'yes')
    return env


@contextmanager
def row_block_format(env, enabled):
    conn = env.getConnection()
    previous = config_value(conn)
    try:
        env.assertEqual(conn.execute_command('CONFIG', 'SET', ROW_BLOCK_CONFIG, enabled), 'OK')
        yield
    finally:
        conn.execute_command('CONFIG', 'SET', ROW_BLOCK_CONFIG, previous)


def row_block_modes(env):
    """Yields 'no' with the flag turned off, then 'yes' with it as a `row_block_env` started."""
    with row_block_format(env, 'no'):
        yield 'no'
    env.assertEqual(config_value(env.getConnection()), 'yes')
    yield 'yes'


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
            kwargs['connection_class'] = env.getConnection(shard).connection_pool.connection_class
            pools.append(redis.ConnectionPool(**kwargs))
            conn = redis.Redis(connection_pool=pools[-1])
            conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
            conns.append(conn)
        yield conns
    finally:
        for pool in pools:
            pool.disconnect()


def read_wire_reply(stream):
    """Keep RESP markers and framing that redis-py's decoded replies discard."""
    line = stream.readline()
    if not line.endswith(b'\r\n'):
        raise AssertionError(f'Incomplete RESP header: {line!r}')
    marker, value = line[:1], line[1:-2]
    if marker in (b'*', b'%'):
        count = int(value) * (2 if marker == b'%' else 1)
        return line + b''.join(read_wire_reply(stream) for _ in range(max(count, 0)))
    if marker == b'$':
        length = int(value)
        if length < 0:
            return line
        payload = stream.read(length + 2)
        if len(payload) != length + 2 or not payload.endswith(b'\r\n'):
            raise AssertionError('Incomplete RESP bulk string')
        return line + payload
    if marker not in (b'+', b':', b',', b'_', b'#'):
        raise AssertionError(f'Unexpected RESP header: {line!r}')
    return line


@contextmanager
def internal_shard_wire_connection(shard):
    """Use the shard's transport/authentication settings on a dedicated internal socket."""
    kwargs = dict(shard.connection_pool.connection_kwargs)
    kwargs['connection_class'] = shard.connection_pool.connection_class
    pool = redis.ConnectionPool(**kwargs)
    connection = pool.get_connection('_FT.AGGREGATE')
    try:
        connection.send_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        connection.read_response()
        with connection._sock.makefile('rb') as stream:
            def execute(*args):
                connection.send_command(*args)
                return read_wire_reply(stream)
            yield execute
    finally:
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


def coordinator_network_profile(env, profile):
    if env.protocol == 3:
        coordinator = profile['Profile']['Coordinator']
        return coordinator['Result processors profile'][0]
    coordinator = to_dict(to_dict(profile[1])['Coordinator'])
    return to_dict(coordinator['Result processors profile'][0])


def assert_same_as_legacy(env, *query, normalize=lambda reply: reply):
    """Asserts the coordinator replies `query` identically with blocks off and on."""
    replies = {}
    for enabled in row_block_modes(env):
        replies[enabled] = normalize(env.cmd(*query))
    env.assertEqual(replies['yes'], replies['no'], message=query)
    return replies['yes']


def assert_blocks_used(env, missing, query, *args):
    """Asserts the shards replied as blocks: a block row counts every column, a RESP row only the fields it has, so
    `Fields converted` grows by exactly the `missing` absent fields."""
    counts = {}
    for enabled in row_block_modes(env):
        profile = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', query, *args)
        counts[enabled] = coordinator_network_profile(env, profile)['Fields converted']
    env.assertEqual(counts['yes'] - counts['no'], missing, message=(query, args, counts))


def add_docs(env, count, key=lambda i: f'doc:{i}', fields=lambda i: ['n', i]):
    """Hashes keyed without a hash tag by default, so they spread across every shard."""
    conn = getConnectionByEnv(env)
    with conn.pipeline(transaction=False) as pipe:
        for i in range(count):
            pipe.execute_command('HSET', key(i), *fields(i))
        pipe.execute()


def with_optional(i, *fields):
    """`fields`, plus an `optional` field on odd rows only (see `assert_blocks_used`)."""
    return [*fields, *(['optional', 'present'] if i % 2 else [])]


def without_optional(count):
    """How many of rows `0..count` `with_optional` leaves without the field."""
    return (count + 1) // 2


def row_block_cursor_values(env):
    """Decode strings, numbers and missing fields across shard and client cursors."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT', 'optional', 'TEXT').ok()
    # Exceed the shard's default chunk size; client COUNT alone does not do so.
    count = 1005
    add_docs(env, count, fields=lambda i: with_optional(i, 'n', i, 'text', f'value\x00{i}'))
    assert_keys_on_every_shard(env)

    expected = [dict(n=str(i), text=f'value\x00{i}',
                     **({'optional': 'present'} if i % 2 else {})) for i in range(count)]
    for enabled in row_block_modes(env):
        reply, cursor = env.cmd(
            'FT.AGGREGATE', 'idx', '*', 'LOAD', 3, '@n', '@text', '@optional',
            'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, count, 'WITHCURSOR', 'COUNT', 127)
        rows = row_block_rows(env, reply)
        env.assertNotEqual(cursor, 0, message=reply)
        while cursor:
            reply, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 127)
            rows.extend(row_block_rows(env, reply))
        env.assertEqual(rows, expected, message=enabled)
    assert_blocks_used(env, without_optional(count), '*', 'LOAD', 3, '@n', '@text', '@optional',
                       'LIMIT', 0, count)


def row_block_reducer_arrays(env):
    """Shard TOLIST arrays and numeric partial sums survive binary transport."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'category', 'TAG', 'amount', 'NUMERIC').ok()
    count = 30
    add_docs(env, count, fields=lambda i: ['category', str(i % 3), 'amount', i,
                                           'label', f'label{i}'])
    assert_keys_on_every_shard(env)
    expected = [dict(category=str(group), total=str(sum(range(group, count, 3))),
                     labels=sorted(f'label{i}' for i in range(group, count, 3)))
                for group in range(3)]
    for enabled in row_block_modes(env):
        reply = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@label',
                        'GROUPBY', 1, '@category',
                        'REDUCE', 'SUM', 1, '@amount', 'AS', 'total',
                        'REDUCE', 'TOLIST', 1, '@label', 'AS', 'labels',
                        'SORTBY', 2, '@category', 'ASC')
        rows = row_block_rows(env, reply)
        for row in rows:
            row['labels'].sort()
        env.assertEqual(rows, expected, message=enabled)


def row_block_dynamic_schema_fallback(env):
    """A changing LOAD * schema replays earlier encoded rows without data loss."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'common', 'TEXT').ok()
    # A field per document: each shard's LOAD * schema differs from the others' and grows
    # within its own chunk.
    count = 30
    add_docs(env, count, fields=lambda i: ['common', 'x', f'field{i}', i])
    assert_keys_on_every_shard(env)
    expected = sorted(sorted([('common', 'x'), (f'field{i}', str(i))]) for i in range(count))
    # FAIL loads every row before encoding any, so its block carries the grown schema with
    # null slots instead of falling back; both must yield the same rows.
    for policy in ('return', 'fail'):
        with all_shards_config(env, ON_TIMEOUT_CONFIG, policy):
            for enabled in row_block_modes(env):
                reply = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'LIMIT', 0, count)
                rows = sorted(sorted(row.items()) for row in row_block_rows(env, reply))
                env.assertEqual(rows, expected, message=(policy, enabled))


def row_block_buffered_reply(env):
    """Shards that buffer every row before replying must still send them as one block."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT', 'optional', 'TEXT').ok()
    count = 300
    add_docs(env, count, fields=lambda i: with_optional(i, 'n', i, 'text', f'value{i}'))
    # Every shard must hold rows for each one's buffered reply to count.
    assert_keys_on_every_shard(env)

    query = ['LOAD', 3, '@n', '@text', '@optional', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, count]
    expected = [dict(n=str(i), text=f'value{i}',
                     **({'optional': 'present'} if i % 2 else {})) for i in range(count)]
    shard_query = ['_FT.AGGREGATE', 'idx', '*', *query, row_block_token(env)]
    # Both policies make the shard aggregate all rows before replying (startPipelineCommon);
    # with workers that reply is also deferred to the main thread's reply callback.
    for policy in ('fail', 'return-strict'):
        for workers in (0, 2):
            context = dict(policy=policy, workers=workers)
            with all_shards_config(env, ON_TIMEOUT_CONFIG, policy), \
                 all_shards_config(env, 'search-workers', workers):
                for enabled in row_block_modes(env):
                    reply = env.cmd('FT.AGGREGATE', 'idx', '*', *query)
                    env.assertEqual(row_block_rows(env, reply), expected,
                                    message=(context, enabled))
                assert_blocks_used(env, without_optional(count), '*', *query)

                with internal_shard_connections(env) as shards:
                    for shard in shards:
                        reply = shard.execute_command(*shard_query)
                        results = shard_rows(env, reply)
                        env.assertEqual(len(results), 1, message=context)
                        env.assertTrue(isinstance(results[0], bytes), message=context)
                        if env.protocol == 3:
                            env.assertGreater(reply[b'row_block_rows'], 0, message=context)


def row_block_streaming_reply(env):
    """Shards that stream rows send each chunk as one block holding exactly the LIMITed rows."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    add_docs(env, 300)
    assert_keys_on_every_shard(env)

    limit = 5
    with internal_shard_connections(env) as shards:
        for shard in shards:
            reply = shard.execute_command('_FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                                          'LIMIT', 0, limit, row_block_token(env))
            results = shard_rows(env, reply)
            env.assertEqual(len(results), 1)
            env.assertTrue(isinstance(results[0], bytes))
            if env.protocol == 3:
                env.assertEqual(reply[b'row_block_rows'], limit)


def row_block_mid_chunk_fallback(env):
    """A schema that grows within a chunk makes the shard finish it in RESP, as without the token."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'common', 'TEXT').ok()
    count = 30
    # A field per document: each shard's LOAD * schema grows with every row.
    add_docs(env, count, fields=lambda i: ['common', 'x', f'field{i}', i])
    assert_keys_on_every_shard(env)

    query = ['_FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'LIMIT', 0, count, 'TIMEOUT', 0]
    with all_shards_config(env, ON_TIMEOUT_CONFIG, 'return'), \
         all_shards_config(env, 'search-workers', 0), internal_shard_connections(env) as shards:
        for shard in shards:
            legacy = shard.execute_command(*query)
            reply = shard.execute_command(*query, row_block_token(env))
            env.assertEqual(reply, legacy)
            env.assertFalse(any(isinstance(row, bytes) for row in shard_rows(env, reply)))
            # The first row fixes the schema; the next document introduces a column and
            # forces replay of the first row. Decoded equality alone misses RESP key types.
            with internal_shard_wire_connection(shard) as execute:
                legacy_wire = execute(*query)
                reply_wire = execute(*query, row_block_token(env))
                if env.protocol == 3:
                    env.assertTrue(b'+extra_attributes\r\n' in legacy_wire)
                    env.assertTrue(b'+values\r\n' in legacy_wire)
                env.assertTrue(reply_wire == legacy_wire,
                               message=f'mid-chunk fallback wire mismatch, RESP{env.protocol}')
        # FAIL loads every row before encoding any, so the block carries the grown schema
        # instead of falling back.
        with all_shards_config(env, ON_TIMEOUT_CONFIG, 'fail'):
            for shard in shards:
                results = shard_rows(env, shard.execute_command(*query, row_block_token(env)))
                env.assertEqual(len(results), 1)
                env.assertTrue(isinstance(results[0], bytes))


def row_block_first_row_fallback(env):
    """A row larger than the writer's byte bound must fall back before any row is encoded."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    # Only one shard needs the oversized value; sorting puts it before a normal row.
    payload = b'x\x00\r\n' * (8 * 1024 * 1024) + b'x'
    add_docs(env, 2, key=lambda i: f'{{rowblock-first}}:{i}',
             fields=lambda i: ['n', i, 'payload', payload if i == 0 else 'small'])
    query = ['_FT.AGGREGATE', 'idx', '*', 'LOAD', 2, '@n', '@payload',
             'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, 2, 'TIMEOUT', 0]
    nonempty = 0
    with internal_shard_connections(env) as shards:
        for shard in shards:
            legacy = shard.execute_command(*query)
            if not shard_rows(env, legacy):
                continue
            nonempty += 1
            reply = shard.execute_command(*query, row_block_token(env))
            env.assertEqual(len(shard_rows(env, reply)), 2, message=env.protocol)
            env.assertTrue(reply == legacy, message=f'first-row fallback, RESP{env.protocol}')
            with internal_shard_wire_connection(shard) as execute:
                legacy_wire = execute(*query)
                reply_wire = execute(*query, row_block_token(env))
                env.assertTrue(reply_wire == legacy_wire,
                               message=f'first-row fallback wire mismatch, RESP{env.protocol}')
            # The same columns with an encodable row still use the block path.
            small = shard.execute_command('_FT.AGGREGATE', 'idx', '@n:[1 1]',
                                          'LOAD', 2, '@n', '@payload', row_block_token(env))
            env.assertTrue(isinstance(shard_rows(env, small)[0], bytes), message=small)
    env.assertEqual(nonempty, 1)


def row_block_empty_chunk(env):
    """A chunk with columns but no rows is still a block, with a zero row count."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    add_docs(env, 30)
    with internal_shard_connections(env) as shards:
        for shard in shards:
            reply = shard.execute_command('_FT.AGGREGATE', 'idx', '@n:[1000 1000]', 'LOAD', 1,
                                          '@n', row_block_token(env))
            results = shard_rows(env, reply)
            env.assertEqual(len(results), 1)
            env.assertTrue(isinstance(results[0], bytes))
            if env.protocol == 3:
                env.assertEqual(reply[b'row_block_rows'], 0)


def row_block_unsupported_extras(env):
    """Requests asking for per-row extras a block cannot carry get RESP rows."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 'text', 'TEXT').ok()
    add_docs(env, 30, fields=lambda i: ['n', i, 'text', 'word'])
    assert_keys_on_every_shard(env)

    extras = {
        'WITHRAWIDS': ['WITHRAWIDS'],
        '_REQUIRED_FIELDS': ['_REQUIRED_FIELDS', 1, 'n'],
    }
    with internal_shard_connections(env) as shards:
        for name, extra in extras.items():
            query = ['_FT.AGGREGATE', 'idx', 'word', *extra, 'LOAD', 1, '@n', 'LIMIT', 0, 30]
            for shard in shards:
                legacy = shard.execute_command(*query)
                env.assertEqual(shard.execute_command(*query, row_block_token(env)), legacy,
                                message=name)


def row_block_counts(env):
    """A block counts its rows, even when LIMIT consumes only part of it."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    count = 20
    add_docs(env, count)
    assert_keys_on_every_shard(env)

    def rows_and_total(reply):
        rows = row_block_rows(env, reply)
        return rows, reply['total_results'] if env.protocol == 3 else reply[0]

    sort = ['SORTBY', 2, '@n', 'ASC']
    for head, tail in (([], ['LIMIT', 0, 1]), ([], ['LIMIT', 0, 0]), ([], ['LIMIT', 0, 3]),
                       ([], [*sort, 'LIMIT', 0, 3]),
                       ([], ['FILTER', '@n > 100', 'LIMIT', 0, 10]),
                       (['WITHCOUNT'], [*sort, 'LIMIT', 0, 1]),
                       (['WITHCOUNT'], ['LIMIT', 0, 3])):
        query = ['FT.AGGREGATE', 'idx', '*', *head, 'LOAD', 1, '@n', *tail]
        # Toggled at runtime: the coordinator reads the flag per query.
        with row_block_format(env, 'no'):
            expected, expected_total = rows_and_total(env.cmd(*query))
        with row_block_format(env, 'yes'):
            actual, total = rows_and_total(env.cmd(*query))
        if head:
            env.assertEqual(total, count, message=query)
        if 'SORTBY' in tail:
            env.assertEqual((actual, total), (expected, expected_total), message=query)
        else:
            # Unsorted rows arrive in shard-reply order, which varies between runs, and so
            # do the shard replies a short LIMIT leaves unread, whose totals go unsummed.
            env.assertEqual(len(actual), len(expected), message=query)
            env.assertTrue(all(0 <= int(row['n']) < count for row in actual), message=actual)


def row_block_no_columns(env):
    """With no column to carry, shards reply RESP rows and the replies stay the same."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    count = 30
    add_docs(env, count)
    assert_keys_on_every_shard(env)
    for query in (['*'], ['*', 'WITHCOUNT', 'LIMIT', 0, count]):
        reply = assert_same_as_legacy(env, 'FT.AGGREGATE', 'idx', *query)
        rows = reply['results'] if env.protocol == 3 else reply[1:]
        env.assertEqual(len(rows), query[-1] if 'LIMIT' in query else count, message=query)

    # The shard part of GROUPBY 0 carries the partial count as its single column.
    reply = assert_same_as_legacy(env, 'FT.AGGREGATE', 'idx', '*',
                                  'GROUPBY', 0, 'REDUCE', 'COUNT', 0, 'AS', 'count')
    env.assertEqual(row_block_rows(env, reply), [{'count': str(count)}])

    with internal_shard_connections(env) as shards:
        for shard in shards:
            reply = shard.execute_command('_FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, count,
                                          row_block_token(env))
            rows = shard_rows(env, reply)
            env.assertEqual(len(rows), shard.execute_command('DBSIZE'))
            env.assertFalse(any(isinstance(row, bytes) for row in rows), message=rows)


@skip(cluster=False)
def test_row_block_buffered_reply_resp3():
    row_block_buffered_reply(row_block_env(protocol=3))


@skip(cluster=False)
def test_row_block_buffered_reply():
    row_block_buffered_reply(row_block_env())


@skip(cluster=False)
def test_row_block_cursor_values_resp3():
    row_block_cursor_values(row_block_env(protocol=3))


@skip(cluster=False)
def test_row_block_cursor_values():
    row_block_cursor_values(row_block_env())


@skip(cluster=False)
def test_row_block_reducer_arrays_resp3():
    row_block_reducer_arrays(row_block_env(protocol=3))


@skip(cluster=False)
def test_row_block_reducer_arrays():
    row_block_reducer_arrays(row_block_env())


@skip(cluster=False)
def test_row_block_dynamic_schema_fallback_resp3():
    row_block_dynamic_schema_fallback(row_block_env(protocol=3))


@skip(cluster=False)
def test_row_block_dynamic_schema_fallback():
    row_block_dynamic_schema_fallback(row_block_env())


@skip(cluster=False)
def test_row_block_resp3_counts():
    row_block_counts(row_block_env(protocol=3))


@skip(cluster=False)
def test_row_block_counts():
    row_block_counts(row_block_env())


@skip(cluster=False)
def test_row_block_streaming_reply_resp3():
    row_block_streaming_reply(Env(protocol=3))


@skip(cluster=False)
def test_row_block_streaming_reply():
    row_block_streaming_reply(Env())


@skip(cluster=False)
def test_row_block_mid_chunk_fallback_resp3():
    row_block_mid_chunk_fallback(Env(protocol=3))


@skip(cluster=False)
def test_row_block_mid_chunk_fallback():
    row_block_mid_chunk_fallback(Env())


@skip(cluster=False)
def test_row_block_first_row_fallback_resp3():
    row_block_first_row_fallback(Env(protocol=3))


@skip(cluster=False)
def test_row_block_first_row_fallback():
    row_block_first_row_fallback(Env())


@skip(cluster=False)
def test_row_block_empty_chunk_resp3():
    row_block_empty_chunk(Env(protocol=3))


@skip(cluster=False)
def test_row_block_empty_chunk():
    row_block_empty_chunk(Env())


@skip(cluster=False)
def test_row_block_unsupported_extras_resp3():
    row_block_unsupported_extras(Env(protocol=3))


@skip(cluster=False)
def test_row_block_unsupported_extras():
    row_block_unsupported_extras(Env())


@skip(cluster=False)
def test_row_block_token_is_internal_only():
    """A user command must not accept the internal arguments."""
    env = Env()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    for token in ('_ROW_BLOCK', '_ROW_BLOCK_RESP3'):
        env.expect('FT.AGGREGATE', 'idx', '*', token).error()


@skip(cluster=False)
def test_row_block_no_columns_resp3():
    row_block_no_columns(row_block_env(protocol=3))


@skip(cluster=False)
def test_row_block_no_columns():
    row_block_no_columns(row_block_env())


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


def row_block_capacity_fallback(env):
    """A full block replays earlier rows and cursors retry encoding their next chunk."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    payload = 'x' * (8 * 1024 * 1024)
    add_docs(env, 5, key=lambda i: f'{{rowblock-cap}}:{i}',
             fields=lambda i: ['n', i, 'payload', payload if i < 4 else 'small'])
    query = ['_FT.AGGREGATE', 'idx', '*', 'LOAD', 2, '@n', '@payload',
             'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, 5, 'TIMEOUT', 0]
    for policy in ('return', 'fail', 'return-strict'):
        for workers in (0, 2):
            context = dict(policy=policy, workers=workers, protocol=env.protocol)
            with all_shards_config(env, ON_TIMEOUT_CONFIG, policy), \
                 all_shards_config(env, 'search-workers', workers), \
                 internal_shard_connections(env) as shards:
                for shard in shards:
                    legacy = shard.execute_command(*query)
                    if not shard_rows(env, legacy):
                        continue
                    reply = shard.execute_command(*query, row_block_token(env))
                    env.assertEqual(reply, legacy, message=context)
                    env.assertEqual(len(shard_rows(env, reply)), 5, message=context)

                    first, cursor = shard.execute_command(*query, row_block_token(env),
                                                          'WITHCURSOR', 'COUNT', 4)
                    env.assertEqual(shard_rows(env, first), shard_rows(env, legacy)[:4],
                                    message=context)
                    env.assertGreater(cursor, 0, message=context)
                    second, cursor = shard.execute_command('_FT.CURSOR', 'READ', 'idx', cursor)
                    rows = shard_rows(env, second)
                    env.assertEqual(len(rows), 1, message=context)
                    env.assertTrue(isinstance(rows[0], bytes), message=context)
                    if env.protocol == 3:
                        env.assertEqual(second[b'row_block_rows'], 1, message=context)
                    # Some pipelines discover EOF on the following read.
                    if cursor:
                        shard.execute_command('_FT.CURSOR', 'DEL', 'idx', cursor)
                    # A separate request must also receive its own clean writer.
                    small = shard.execute_command('_FT.AGGREGATE', 'idx', '@n:[4 4]',
                                                  'LOAD', 2, '@n', '@payload', row_block_token(env))
                    env.assertTrue(isinstance(shard_rows(env, small)[0], bytes), message=context)


@skip(cluster=False)
def test_row_block_capacity_fallback():
    row_block_capacity_fallback(Env())


@skip(cluster=False)
def test_row_block_capacity_fallback_resp3():
    row_block_capacity_fallback(Env(protocol=3))
