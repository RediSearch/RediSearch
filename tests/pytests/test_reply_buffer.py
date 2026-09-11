# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from RLTest import Env
from common import (debug_cmd, getConnectionByEnv, run_command_on_all_shards,
                    skip, skipIfNoEnableAssert, to_dict, wait_for_condition)


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


def _exercise_interrupted_serialization(protocol, coordinator, hybrid=False):
    """Interrupt an open row, then check reply framing and blocked-client cleanup."""
    import redis
    import struct
    import threading

    env = Env(protocol=protocol, moduleArgs='WORKERS 2 TIMEOUT 60000')
    skipIfNoEnableAssert(env)
    schema = ['n', 'NUMERIC', 'SORTABLE']
    if hybrid:
        schema += ['v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
                   'DISTANCE_METRIC', 'L2']
    env.expect('FT.CREATE', 'idx', 'SCHEMA', *schema).ok()
    conn = getConnectionByEnv(env)
    for n in range(5):
        conn.execute_command('HSET', f'{{doc}}:{n}', 'n', n,
                             'v', struct.pack('ff', n, n))
    if hybrid:
        hook = 'DuringHybridRowSerialization'
        query = ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
                 'SORTBY', 2, '@n', 'ASC', 'LOAD', 1, '@n',
                 'PARAMS', 2, 'BLOB', struct.pack('ff', 0, 0)]
    elif coordinator:
        hook = 'DuringCoordRowSerialization'
        query = ['FT.SEARCH', 'idx', '*', 'WITHSCORES', 'EXPLAINSCORE', 'RETURN', 1, 'n']
    else:
        hook = 'DuringRowSerialization'
        query = ['FT.AGGREGATE', 'idx', '*', 'SORTBY', 2, '@n', 'ASC', 'LOAD', 1, '@n']
    free_counter = ('GET_COORD_SEARCH_ONFREE_COUNT' if coordinator
                    else 'GET_BLOCKED_REQUEST_ONFREE_COUNT')

    def assert_rows(result, timed_out=False):
        if hybrid:
            rows = (result if protocol == 3 else to_dict(result))['results']
            fields = rows if protocol == 3 else [to_dict(row) for row in rows]
            # HYBRID stops at the completed row; its tail cannot be drained by the callback.
            env.assertEqual([int(row['n']) for row in fields],
                            [0] if timed_out else list(range(5)))
        elif coordinator:
            ids = ([row['id'] for row in result['results']] if protocol == 3
                   else result[1::3])
            env.assertEqual(sorted(ids), [f'{{doc}}:{n}' for n in range(5)])
        else:
            fields = ([row['extra_attributes'] for row in result['results']]
                      if protocol == 3 else [to_dict(row) for row in result[1:]])
            env.assertEqual([int(row['n']) for row in fields], list(range(5)))

    for policy in ('FAIL', 'RETURN-STRICT'):
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', policy)
        for action in ('timeout', 'disconnect'):
            pool = env.getConnection().connection_pool
            # A raw connection prevents redis-py from retrying the killed query.
            client = pool.get_connection()
            client.send_command('CLIENT', 'ID')
            client_id = client.read_response()
            free_before = env.cmd(debug_cmd(), 'QUERY_CONTROLLER', free_counter)
            outcome = []

            def run_query():
                try:
                    client.send_command(*query)
                    outcome.append(client.read_response())
                except redis.RedisError as error:
                    outcome.append(error)

            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', hook).ok()
            worker = threading.Thread(target=run_query, daemon=True)
            worker.start()
            try:
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', hook) == 1, {}),
                    'Row serialization did not reach its open collection', timeout=10)
                if action == 'timeout':
                    if coordinator:
                        stats_before = env.cmd('INFO', 'MODULES')
                    env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                else:
                    env.expect('CLIENT', 'KILL', 'ID', client_id).equal(1)
                worker.join(timeout=10)
                env.assertFalse(worker.is_alive())
                env.assertEqual(len(outcome), 1)
                if action == 'disconnect':
                    env.assertIsInstance(outcome[0], redis.ConnectionError)
                elif policy == 'FAIL':
                    env.assertIsInstance(outcome[0], redis.ResponseError)
                    env.assertContains('Timeout limit was reached', str(outcome[0]))
                else:
                    assert_rows(outcome[0], timed_out=True)
                if action == 'timeout':
                    client.send_command('PING')
                    env.assertEqual(client.read_response(), 'PONG')
                    if coordinator and not hybrid:
                        env.assertEqual(env.cmd(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', hook),
                                        1 if policy == 'FAIL' else 5)
                        stats_after = env.cmd('INFO', 'MODULES')
                        kind = 'errors' if policy == 'FAIL' else 'warnings'
                        prefix = f'search_coord_total_query_{kind}_timeout_while_'
                        env.assertEqual(stats_after[prefix + 'replying'],
                                        stats_before[prefix + 'replying'] + 1)
                        env.assertEqual(stats_after[prefix + 'executing'],
                                        stats_before[prefix + 'executing'])
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'QUERY_CONTROLLER', free_counter) > free_before, {}),
                    'Blocked reply buffer owner was not freed', timeout=10)
            finally:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', hook)
                worker.join(timeout=10)
                client.disconnect()
                pool.release(client)
            env.assertEqual(env.cmd('INFO', 'clients')['blocked_clients'], 0)
            assert_rows(env.cmd(*query))


@skip(cluster=True)
def test_interrupted_worker_serialization_resp2():
    _exercise_interrupted_serialization(2, False)


@skip(cluster=True)
def test_interrupted_worker_serialization_resp3():
    _exercise_interrupted_serialization(3, False)


@skip(cluster=False, min_shards=2)
def test_interrupted_coordinator_serialization_resp2():
    _exercise_interrupted_serialization(2, True)


@skip(cluster=False, min_shards=2)
def test_interrupted_coordinator_serialization_resp3():
    _exercise_interrupted_serialization(3, True)


def test_interrupted_hybrid_serialization_resp2():
    _exercise_interrupted_serialization(2, False, hybrid=True)


def test_interrupted_hybrid_serialization_resp3():
    _exercise_interrupted_serialization(3, False, hybrid=True)


def _exercise_coordinator_early_cleanup(phase):
    """Verify handle cleanup when execution terminates before the reducer runs."""
    import redis
    import threading

    env = Env(moduleArgs='WORKERS 2 TIMEOUT 60000 ON_TIMEOUT FAIL')
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    getConnectionByEnv(env).execute_command('HSET', '{doc}:0', 'n', 0)
    free_counter = 'GET_COORD_SEARCH_ONFREE_COUNT'

    for action in (('drop',) if phase == 'prepare' else ('timeout', 'disconnect')):
        pool = env.getConnection().connection_pool
        client = pool.get_connection()
        client.send_command('CLIENT', 'ID')
        client_id = client.read_response()
        free_before = env.cmd(debug_cmd(), 'QUERY_CONTROLLER', free_counter)
        stats_before = env.cmd('INFO', 'COMMANDSTATS').get('cmdstat_FT.SEARCH',
                                                         {'calls': 0, 'usec': 0})
        stats_at_timeout = None
        outcome = []

        def run_query():
            try:
                client.send_command('FT.SEARCH', 'idx', '*')
                outcome.append(client.read_response())
            except redis.RedisError as error:
                outcome.append(error)

        if phase == 'fanout':
            run_command_on_all_shards(env, debug_cmd(), 'WORKERS', 'PAUSE')
        else:
            env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'IS_PAUSED') == 1, {}),
                'Coordinator pool did not pause')
        worker = threading.Thread(target=run_query, daemon=True)
        worker.start()
        try:
            wait_for_condition(
                lambda: (any(c['id'] == str(client_id) and 'b' in c['flags']
                             for c in env.getConnection().client_list()), {}),
                'Coordinator client did not block')
            if phase == 'fanout':
                wait_for_condition(
                    lambda: (all(any(c['cmd'].lower() == '_ft.search' and 'b' in c['flags']
                                     for c in shard.client_list())
                                 for shard in env.getOSSMasterNodesConnectionList()), {}),
                    'Fanout did not reach the paused shard workers')
            if action == 'timeout':
                # Measure a known blocked interval; the timeout itself is explicitly triggered.
                env.cmd('DEBUG', 'SLEEP', 0.05)
                env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                stats_at_timeout = env.cmd('INFO', 'COMMANDSTATS')['cmdstat_FT.SEARCH']
                env.assertEqual(stats_at_timeout['calls'], stats_before['calls'] + 1)
                env.assertGreaterEqual(stats_at_timeout['usec'], stats_before['usec'] + 40000,
                                       message=stats_at_timeout)
            elif action == 'disconnect':
                env.expect('CLIENT', 'KILL', 'ID', client_id).equal(1)
            else:
                # Remove the local spec so preparation cannot promote the queued weak ref.
                env.expect('FLUSHDB').equal(True)
        finally:
            if phase == 'fanout':
                run_command_on_all_shards(env, debug_cmd(), 'WORKERS', 'RESUME')
            else:
                env.cmd(debug_cmd(), 'COORD_THREADS', 'RESUME')
            worker.join(timeout=10)
            client.disconnect()
            pool.release(client)

        env.assertFalse(worker.is_alive())
        env.assertEqual(len(outcome), 1)
        if action == 'disconnect':
            env.assertIsInstance(outcome[0], redis.ConnectionError)
        else:
            env.assertIsInstance(outcome[0], redis.ResponseError)
            env.assertContains('dropped' if action == 'drop' else 'Timeout limit was reached',
                               str(outcome[0]))
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'QUERY_CONTROLLER', free_counter) > free_before, {}),
            f'Coordinator request leaked during {phase}/{action}')
        if stats_at_timeout is not None:
            stats_after = env.cmd('INFO', 'COMMANDSTATS')['cmdstat_FT.SEARCH']
            env.assertEqual((stats_after['calls'], stats_after['usec']),
                            (stats_at_timeout['calls'], stats_at_timeout['usec']))


@skip(cluster=False, min_shards=2)
def test_coordinator_cleanup_before_pickup():
    _exercise_coordinator_early_cleanup('queued')


@skip(cluster=False, min_shards=2)
def test_coordinator_cleanup_during_fanout():
    _exercise_coordinator_early_cleanup('fanout')


@skip(cluster=False, min_shards=2)
def test_coordinator_cleanup_after_preparation_failure():
    _exercise_coordinator_early_cleanup('prepare')


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
