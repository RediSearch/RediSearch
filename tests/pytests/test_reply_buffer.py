# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from RLTest import Env
from common import (debug_cmd, getConnectionByEnv, run_command_on_all_shards,
                    skip, skipIfNoEnableAssert, to_dict)


def _exercise_reply_buffers(protocol):
    # Compare foreground replies with the protocol captured by blocked-client buffers.
    env = Env(protocol=protocol, moduleArgs='WORKERS 2 TIMEOUT 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT', 'SORTABLE', 'UNF').ok()
    conn = getConnectionByEnv(env)
    values = ['short', 'x' * 65536, 'middle', 'last']
    for n, value in enumerate(values):
        conn.execute_command('HSET', f'{{doc}}:{n}', 'n', n, 'text', value,
                             'raw', value + '\x00tail')
    search = ['FT.SEARCH', 'idx', '*', 'SORTBY', 'n', 'ASC',
              'RETURN', 3, 'n', 'text', 'raw', 'LIMIT', 0, 10]
    aggregate = ['FT.AGGREGATE', 'idx', '*', 'SORTBY', 2, '@n', 'ASC',
                 'LOAD', 3, '@n', '@text', '@raw']
    expected = None
    try:
        for workers in (0, 2):
            run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-workers', workers)
            for policy in ('return', 'fail', 'return-strict'):
                run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', policy)
                replies = [env.cmd(*search), env.cmd(*aggregate)]
                if expected is None:
                    expected = replies
                env.assertEqual(replies, expected)
                result = replies[0]
                if protocol == 3:
                    rows = result['results']
                    env.assertEqual([row['id'] for row in rows],
                                    [f'{{doc}}:{n}' for n in range(4)])
                    fields = [row['extra_attributes'] for row in rows]
                else:
                    env.assertEqual(result[0], 4, message=result)
                    env.assertEqual(result[1::2], [f'{{doc}}:{n}' for n in range(4)])
                    fields = [to_dict(row) for row in result[2::2]]
                env.assertEqual([int(row['n']) for row in fields], list(range(4)))
                env.assertEqual([row['text'] for row in fields], values)
                env.assertEqual([row['raw'] for row in fields], [value + '\x00tail' for value in values])
                if not env.isCluster():
                    pipeline = env.getConnection().pipeline(transaction=True)
                    pipeline.execute_command(*search)
                    env.assertEqual(pipeline.execute(), [result])
                    # Lua returns RESP2 arrays unless the script selects RESP3.
                    script = (f'redis.setresp({protocol}); '
                              "return redis.call(unpack(ARGV))")
                    env.assertEqual(env.cmd('EVAL', script, 0, *search), result)
                env.assertTrue(env.cmd('PING'))
    finally:
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-workers', 2)
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', 'return')


def test_reply_buffers_resp2():
    """Rows keep their values through foreground, worker, shard and coordinator serialization."""
    _exercise_reply_buffers(2)


def test_reply_buffers_resp3():
    """Native RESP3 fragments keep map lengths and binary strings across reused rows."""
    _exercise_reply_buffers(3)


def _exercise_return_reply_compatibility(protocol, oom_policy='RETURN'):
    env = Env(protocol=protocol,
              moduleArgs=f'WORKERS 2 TIMEOUT 1000 ON_TIMEOUT RETURN ON_OOM {oom_policy}')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'ord', 'NUMERIC', 'SORTABLE',
               'val', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for n, value in enumerate(('10', 'oops', '20')):
        conn.execute_command('HSET', f'{{doc}}:{n}', 'ord', n, 'val', value)
    plain = ['FT.AGGREGATE', 'idx', '*']
    expression = ['FT.AGGREGATE', 'idx', '*', 'SORTBY', 2, '@ord', 'ASC',
                  'APPLY', '@val + 0', 'AS', 'computed']
    expected = None
    for workers in (0, 2):
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-workers', workers)
        replies = [env.cmd(*plain), env.cmd(*expression)]
        if expected is None:
            expected = replies
        env.assertEqual(replies, expected)
        if not env.isCluster():
            if protocol == 2:
                env.assertEqual(replies[0], [1, [], [], []])
                env.assertEqual(replies[1], [3, ['ord', '0', 'val', '10', 'computed', '10']])
            else:
                env.assertEqual(len(replies[1]['results']), 1)
                env.assertContains('Invalid numeric value', replies[1]['warning'][0])
        # A failure before any successful row still produces a top-level error.
        conn.execute_command('HSET', '{doc}:0', 'val', 'oops')
        env.expect(*expression).error().contains('Invalid numeric value')
        conn.execute_command('HSET', '{doc}:0', 'val', '10')
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-max-aggregate-results', 0)
    expected_count = None
    for workers in (0, 2):
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-workers', workers)
        result = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 0)
        if expected_count is None:
            expected_count = result
        env.assertEqual(result, expected_count)
        if not env.isCluster():
            env.assertEqual(result if protocol == 2 else result['total_results'],
                            [3] if protocol == 2 else 3)


def test_return_reply_compatibility_resp2():
    _exercise_return_reply_compatibility(2)


def test_return_reply_compatibility_resp3():
    _exercise_return_reply_compatibility(3)


def test_ignore_oom_reply_compatibility_resp2():
    _exercise_return_reply_compatibility(2, 'IGNORE')


def test_ignore_oom_reply_compatibility_resp3():
    _exercise_return_reply_compatibility(3, 'IGNORE')


def _exercise_timeout_reply_policies(protocol):
    env = Env(protocol=protocol, moduleArgs='WORKERS 2 TIMEOUT 1000 ON_TIMEOUT RETURN')
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    for n in range(5):
        env.cmd('HSET', str(n), 'n', n)
    query = [debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
             'TIMEOUT_AFTER_N', 2, 'DEBUG_PARAMS_COUNT', 2]
    for oom_policy in ('RETURN', 'IGNORE', 'FAIL'):
        env.cmd('CONFIG', 'SET', 'search-on-oom', oom_policy)
        env.cmd('CONFIG', 'SET', 'search-workers', 0)
        expected = env.cmd(*query)
        env.cmd('CONFIG', 'SET', 'search-workers', 2)
        result = env.cmd(*query)
        env.assertEqual(result, expected)
        rows = result[1:] if protocol == 2 else result['results']
        env.assertEqual(len(rows), 0 if oom_policy == 'FAIL' else 2)


@skip(cluster=True)
def test_timeout_reply_policies_resp2():
    _exercise_timeout_reply_policies(2)


@skip(cluster=True)
def test_timeout_reply_policies_resp3():
    _exercise_timeout_reply_policies(3)


def test_reply_buffer_cursor_protocol_changes():
    """Each cursor read captures its reader's protocol and publishes each row once."""
    import redis
    env = Env(moduleArgs='WORKERS 2 ON_TIMEOUT FAIL TIMEOUT 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for n in range(7):
        conn.execute_command('HSET', f'{{doc}}:{n}', 'n', n)
    kwargs = dict(env.getConnection().connection_pool.connection_kwargs)
    kwargs['protocol'] = 3
    other = redis.Redis(connection_pool=redis.ConnectionPool(**kwargs))
    try:
        first, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'SORTBY', 2, '@n', 'ASC',
                                 'WITHCURSOR', 'COUNT', 1)
        found = [int(to_dict(row)['n']) for row in first[1:]]
        protocol = 3
        while cursor:
            client = other if protocol == 3 else env.getConnection()
            result, cursor = client.execute_command('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 1)
            fields = ([row['extra_attributes'] for row in result['results']]
                      if protocol == 3 else [to_dict(row) for row in result[1:]])
            found.extend(int(row['n']) for row in fields)
            protocol = 5 - protocol
        env.assertEqual(found, list(range(7)))
        env.assertEqual(other.ping(), True)
    finally:
        other.close()
        other.connection_pool.disconnect()
