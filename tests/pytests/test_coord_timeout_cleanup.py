# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import threading

from redis import ConnectionPool, Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from common import *
from test_blocked_client_timeout import _get_blocked_request_onfree_count, is_client_blocked


def _exercise_cleanup(stage, debug_query=False, hold_worker=False, protocol=3, real_timeout=False,
                      profile=False):
    # Force each cancellation at a particular coordinator ownership transition.
    # GC retains its own weak reference until its next timer, even after DROPINDEX.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0 NOGC', protocol=protocol)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    points = {
        'prepare': 'BeforeCoordSearchPrepare',
        'prepared': 'AfterCoordSearchPrepare',
        'fanout': 'BeforeCoordFanout',
        'empty_fanout': 'BeforeCoordFanout',
        'reducer_claim': 'BeforeCoordReducerClaim',
        'claimed': 'CoordSearchReducerClaimed',
        'serialized': 'CoordSearchReplySerialized',
        'serializing': 'CoordSearchReplyStarted',
    }
    point = points.get(stage)
    cleanup_point = 'CoordSearchRequestFree'
    worker_done_point = 'CoordSearchWorkerDone'
    policies = ('return',) if debug_query else ('return', 'fail')
    if stage == 'claimed' or real_timeout:
        policies = ('fail',)
    for policy in policies:
        cancellations = ('timeout',) if real_timeout else (
            ('disconnect',) if policy == 'return' else ('timeout', 'disconnect'))
        for cancellation in cancellations:
            env.expect('CONFIG', 'SET', 'search-on-timeout', policy).ok()
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
            getConnectionByEnv(env).execute_command('HSET', '{doc}:1', 'name', 'hello')
            original = env.getConnection().connection_pool
            kwargs = dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0))
            pool = ConnectionPool(connection_class=original.connection_class, **kwargs)
            client = Redis(connection_pool=pool, single_connection_client=True)
            client_id = client.client_id()
            results, errors = [], []

            command = ['FT.SEARCH', 'idx', '*', 'NOCONTENT']
            if real_timeout:
                command += ['TIMEOUT', 5000]
            if profile:
                command = ['FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *command[2:]]
            if debug_query:
                command = [debug_cmd(), *command, 'TIMEOUT_AFTER_N', 1000, 'DEBUG_PARAMS_COUNT', 2]

            def query():
                try:
                    results.append(client.execute_command(*command))
                except Exception as error:
                    errors.append(error)

            free_count_before = _get_blocked_request_onfree_count(env)
            # Shared OnFree also runs for local shard requests; observe this
            # coordinator request's destructor separately.
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', cleanup_point, 1).ok()
            if stage == 'prepared':
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'BeforeCoordFanout', 1).ok()
            if hold_worker:
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', worker_done_point).ok()
            if stage in ('queued', 'prepare', 'prepared'):
                # Only this index owns a reference manager on the coordinator.
                # Destruction can run on main, so the observation must self-release.
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'RefManagerFreed', 1).ok()
            coord_paused = stage == 'queued'
            if coord_paused:
                env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'IS_PAUSED') == 1, {}),
                    'Coordinator threads did not pause', timeout=5)
            elif stage == 'reduce':
                setPauseBeforeReduce(env, PAUSE_BEFORE_REDUCER_INIT)
            else:
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
            thread = threading.Thread(target=query, daemon=True)
            try:
                thread.start()
                wait_for_condition(lambda: (is_client_blocked(env, client_id),
                                            {'results': results, 'errors': errors}),
                                   'Query client did not block', timeout=5)
                if stage == 'reduce':
                    wait_for_condition(lambda: (getIsCoordReducePaused(env), {}),
                                       'Reducer did not pause', timeout=5)
                elif point:
                    wait_for_condition(
                        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
                        f'Query did not reach {point}', timeout=5)
                if cancellation == 'timeout':
                    if not real_timeout:
                        env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                else:
                    env.expect('CLIENT', 'KILL', 'ID', client_id).equal(1)
                thread.join(timeout=10 if real_timeout else 5)
                env.assertFalse(thread.is_alive())
                if stage in ('claimed', 'serializing', 'serialized'):
                    env.expect(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point).equal(True)
                    env.expect(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', cleanup_point).equal(0)
                if cancellation == 'disconnect':
                    env.assertEqual(results, [])
                    env.assertEqual(len(errors), 1)
                    env.assertTrue(isinstance(errors[0], redis_exceptions.ConnectionError),
                                   message=errors)
                else:
                    env.assertEqual(policy, 'fail')
                    env.assertEqual(results, [])
                    env.assertEqual(len(errors), 1)
                    env.assertContains('Timeout limit was reached', str(errors[0]))
                if stage in ('serializing', 'serialized') and cancellation == 'timeout':
                    env.assertTrue(client.ping())
                if stage == 'prepare':
                    # Preparation now fails after the timeout already owns the reply.
                    env.expect('FT.DROPINDEX', 'idx').ok()
                elif stage == 'empty_fanout':
                    env.expect(debug_cmd(), 'SEND_ERROR', env.shardsCount).ok()
                if stage == 'queued':
                    env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
                    coord_paused = False
                elif point:
                    env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
                elif stage == 'reduce' and policy == 'return':
                    # RETURN has no abort flag; disconnected work finishes normally.
                    resetCoordReduceDebug(env)
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', cleanup_point) == 1, {}),
                    'Redis never freed the completed blocked query', timeout=5)
                env.assertGreater(_get_blocked_request_onfree_count(env), free_count_before)
                if stage == 'prepared':
                    env.expect(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', 'BeforeCoordFanout').equal(
                        1 if policy == 'return' else 0)
                if hold_worker:
                    wait_for_condition(
                        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', worker_done_point), {}),
                        'Worker did not retain its transport reference through request cleanup', timeout=5)
                env.assertTrue(env.cmd('PING'))
                if stage in ('serializing', 'serialized') and cancellation == 'timeout':
                    env.assertTrue(client.ping())
            finally:
                if hold_worker:
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', worker_done_point)
                if coord_paused:
                    env.cmd(debug_cmd(), 'COORD_THREADS', 'RESUME')
                elif stage == 'reduce':
                    resetCoordReduceDebug(env)
                elif point:
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=5)
                env.cmd(debug_cmd(), 'SEND_ERROR', 0)
                client.close()
                pool.disconnect()
                try:
                    if stage != 'prepare':
                        env.expect('FT.DROPINDEX', 'idx').ok()
                    if stage in ('queued', 'prepare', 'prepared'):
                        wait_for_condition(
                            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT',
                                             'RefManagerFreed') == 1, {}),
                            'Cancelled query leaked its index reference manager', timeout=5)
                finally:
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                getConnectionByEnv(env).execute_command('DEL', '{doc}:1')


@skip(cluster=False)
def test_timeout_cleanup_before_dispatch():
    """A cancelled queued query must release its blocked handle and index reference."""
    _exercise_cleanup('queued')


@skip(cluster=False)
def test_timeout_cleanup_prepare_error():
    """Preparation failure after cancellation must release the handle and index reference."""
    _exercise_cleanup('prepare')


@skip(cluster=False)
def test_timeout_cleanup_after_prepare():
    """Cancellation after preparation skips fanout; RETURN disconnect still completes it."""
    _exercise_cleanup('prepared')


@skip(cluster=False)
def test_disconnect_cleanup_debug_after_prepare():
    """Debug search preserves RETURN fanout and shared cleanup after disconnect."""
    _exercise_cleanup('prepared', debug_query=True)


@skip(cluster=False)
def test_timeout_cleanup_fanout():
    """The last shard reply must finish a cancelled fanout."""
    _exercise_cleanup('fanout')


@skip(cluster=False)
def test_timeout_cleanup_empty_fanout():
    """A cancelled fanout with no sent commands has no future reply to finish it."""
    _exercise_cleanup('empty_fanout')


@skip(cluster=False)
def test_timeout_cleanup_reducer_claim():
    """A worker cancelled before reduction still owes blocked completion."""
    _exercise_cleanup('reducer_claim')


@skip(cluster=False)
def test_timeout_cleanup_during_reduce():
    """A reducer that observes cancellation must finish its blocked handle."""
    _exercise_cleanup('reduce')


@skip(cluster=False)
def test_disconnect_cleanup_debug_before_dispatch():
    """A cancelled queued debug search must also release its index reference."""
    _exercise_cleanup('queued', debug_query=True)


@skip(cluster=False)
def test_request_cleanup_before_worker_release():
    """The worker may release its MRCtx reference after the request has been freed."""
    _exercise_cleanup('queued', hold_worker=True)


@skip(cluster=False)
def test_request_cleanup_before_debug_worker_release():
    """The debug worker must also finish without accessing the freed request."""
    _exercise_cleanup('queued', debug_query=True, hold_worker=True)


@skip(cluster=False)
def test_fail_timeout_does_not_wait_for_reducer():
    """FAIL replies while the reducer is parked; request cleanup waits for worker unblocking."""
    _exercise_cleanup('claimed')


@skip(cluster=False)
def test_search_uses_captured_timeout_policy():
    """Config changes after dispatch must not change how search handles shard timeout errors."""
    # Disable real deadlines; VecSim supplies deterministic shard timeouts.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', 6,
               'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    vector = np.array([1.0, 2.0], dtype=np.float32).tobytes()
    getConnectionByEnv(env).execute_command('HSET', '{doc}:1', 'vec', vector)
    command = ['FT.SEARCH', 'idx', '*=>[KNN 1 @vec $v]', 'PARAMS', 2, 'v', vector,
               'NOCONTENT', 'DIALECT', 2]
    prepare_point = 'AfterCoordSearchPrepare'
    reducer_point = 'BeforeCoordReducerClaim'
    previous_policy = env.cmd('CONFIG', 'GET', 'search-on-timeout')['search-on-timeout']
    original = env.getConnection().connection_pool
    kwargs = dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0))
    pool = ConnectionPool(connection_class=original.connection_class, **kwargs)
    client = Redis(connection_pool=pool, single_connection_client=True)
    thread = None
    try:
        with vecsimMockTimeoutContext(env):
            for initial_policy, next_policy in (('fail', 'return'), ('return', 'fail')):
                env.expect('CONFIG', 'SET', 'search-on-timeout', initial_policy).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', prepare_point).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', reducer_point).ok()
                free_count_before = _get_blocked_request_onfree_count(env)
                results, errors = [], []

                def query():
                    try:
                        results.append(client.execute_command(*command))
                    except Exception as error:
                        errors.append(error)

                thread = threading.Thread(target=query, daemon=True)
                thread.start()
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', prepare_point), {}),
                    'Query did not finish preparation', timeout=5)
                # The coordinator already captured its policy. Every shard, including
                # the local one, must now produce a timeout error for the reducer.
                run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', 'fail')
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', prepare_point).ok()
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', reducer_point), {}),
                    'Shard replies did not reach the reducer', timeout=5)
                env.expect('CONFIG', 'SET', 'search-on-timeout', next_policy).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', reducer_point).ok()
                thread.join(timeout=5)
                env.assertFalse(thread.is_alive())
                if initial_policy == 'fail':
                    env.assertEqual(results, [])
                    env.assertEqual(len(errors), 1, message=errors)
                    env.assertTrue(isinstance(errors[0], redis_exceptions.ResponseError),
                                   message=errors)
                    env.assertContains('Timeout limit was reached', str(errors[0]))
                else:
                    env.assertEqual(errors, [])
                    env.assertEqual(results, [{'attributes': [], 'warning': [], 'total_results': 0,
                                               'format': 'STRING', 'results': []}])
                wait_for_condition(
                    lambda: (_get_blocked_request_onfree_count(env) > free_count_before, {}),
                    'Search did not release its QueryRequest cycle', timeout=5)
                env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        if thread:
            thread.join(timeout=5)
        client.close()
        pool.disconnect()
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', previous_policy)


@skip(cluster=False)
def test_generic_fanout_allows_client_unblock(env):
    """Generic MRCtx requests retain manual unblocking without an automatic timeout."""
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    original = env.getConnection().connection_pool
    kwargs = dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0))
    pool = ConnectionPool(connection_class=original.connection_class, **kwargs)
    client = Redis(connection_pool=pool, single_connection_client=True)
    client_id = client.client_id()
    point = 'BeforeCoordFanout'
    try:
        for command in (['FT.INFO', 'idx'], ['FT._LIST', 'WITHCLUSTERSTATE']):
            for reason in ('TIMEOUT', 'ERROR'):
                results, errors = [], []

                def query():
                    try:
                        results.append(client.execute_command(*command))
                    except Exception as error:
                        errors.append(error)

                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
                thread = threading.Thread(target=query, daemon=True)
                try:
                    thread.start()
                    wait_for_condition(
                        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
                        'Generic fanout did not pause', timeout=5)
                    env.assertTrue(is_client_blocked(env, client_id))
                    env.expect('CLIENT', 'UNBLOCK', client_id, reason).equal(1)
                    thread.join(timeout=5)
                    env.assertFalse(thread.is_alive())
                    env.assertEqual(results, [])
                    expected = ('Timeout calling command' if reason == 'TIMEOUT' else
                                'UNBLOCKED client unblocked via CLIENT UNBLOCK')
                    env.assertEqual([str(error) for error in errors], [expected])
                finally:
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                    thread.join(timeout=5)
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                env.expect(*command).noError()
    finally:
        client.close()
        pool.disconnect()


@skip(cluster=False)
def test_cancel_after_background_search_serialization_resp3():
    """Timeout/disconnect discards a completed BG reply without freeing its active worker."""
    _exercise_cleanup('serialized')


@skip(cluster=False)
def test_cancel_during_background_search_serialization_resp2():
    """Cancellation must discard a partial RESP2 reply while BG finishes writing it."""
    _exercise_cleanup('serializing', protocol=2)


@skip(cluster=False)
def test_cancel_during_background_search_serialization_resp3():
    """Cancellation must discard a partial RESP3 reply while BG finishes writing it."""
    _exercise_cleanup('serializing', protocol=3)


def _exercise_background_search_reply(protocol, profile=False):
    # Both RESP versions must flush the BG buffer even with the timer disabled.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0', protocol=protocol)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    getConnectionByEnv(env).execute_command('HSET', '{doc}:1', 'name', 'hello\0世界' + 'x' * 256)
    point = 'CoordSearchReplySerialized'
    for policy in ('fail', 'return'):
        env.expect('CONFIG', 'SET', 'search-on-timeout', policy).ok()
        for timeout in (0, 60000):
            command = ['FT.SEARCH', 'idx', '*', 'RETURN', 1, 'name', 'TIMEOUT', timeout]
            expected = env.cmd(*command)
            if profile:
                command = ['FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *command[2:]]
            original = env.getConnection().connection_pool
            pool = ConnectionPool(connection_class=original.connection_class,
                                  **dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0)))
            client = Redis(connection_pool=pool, single_connection_client=True)
            results, errors = [], []

            def query():
                try:
                    results.append(client.execute_command(*command))
                except Exception as error:
                    errors.append(error)

            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
            thread = threading.Thread(target=query, daemon=True)
            try:
                thread.start()
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
                    'Search did not serialize on BG', timeout=5)
                env.assertTrue(thread.is_alive())
                env.assertEqual(results, [])
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
                thread.join(timeout=5)
                env.assertFalse(thread.is_alive())
                env.assertEqual(errors, [])
                if profile:
                    env.assertEqual(len(results), 1, message=results)
                    actual = results[0]['Results'] if protocol == 3 else results[0][0]
                    env.assertEqual(actual, expected)
                else:
                    env.assertEqual(results, [expected])
                env.assertTrue(client.ping())
            finally:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=5)
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                client.close()
                pool.disconnect()


@skip(cluster=False)
def test_background_search_reply_resp2():
    """FAIL and RETURN deliver their BG-encoded RESP2 result exactly once."""
    _exercise_background_search_reply(2)


@skip(cluster=False)
def test_background_search_reply_resp3():
    """FAIL and RETURN deliver their BG-encoded RESP3 result exactly once."""
    _exercise_background_search_reply(3)


def _exercise_background_search_error(protocol):
    # Pause after encoding to prove errors are buffered until worker completion.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0 NOGC', protocol=protocol)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    serialized = 'CoordSearchReplySerialized'
    cleanup = 'CoordSearchRequestFree'
    cases = (
        ('shard', None, '@name:(', 'Syntax error'),
        ('prepare', 'BeforeCoordSearchPrepare', '*', 'index was dropped'),
        ('empty_fanout', 'BeforeCoordFanout', '*', 'Could not send query to cluster'),
    )
    for stage, point, query, expected_error in cases:
        for policy, cancel in (('fail', False), ('return', False), ('fail', True)):
            env.expect('CONFIG', 'SET', 'search-on-timeout', policy).ok()
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
            original = env.getConnection().connection_pool
            pool = ConnectionPool(connection_class=original.connection_class,
                                  **dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0)))
            client = Redis(connection_pool=pool, single_connection_client=True)
            client_id = client.client_id()
            results, errors = [], []

            def execute():
                try:
                    results.append(client.execute_command('FT.SEARCH', 'idx', query))
                except Exception as error:
                    errors.append(error)

            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', serialized).ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', cleanup, 1).ok()
            if point:
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
            thread = threading.Thread(target=execute, daemon=True)
            dropped = False
            try:
                thread.start()
                if point:
                    wait_for_condition(
                        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
                        f'Search did not reach {point}', timeout=5)
                    if stage == 'prepare':
                        env.expect('FT.DROPINDEX', 'idx').ok()
                        dropped = True
                    else:
                        env.expect(debug_cmd(), 'SEND_ERROR', env.shardsCount).ok()
                    env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()

                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', serialized), {}),
                    'Error was not serialized on BG', timeout=5)
                env.assertTrue(thread.is_alive())
                env.assertEqual(errors, [])
                if cancel:
                    env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                    thread.join(timeout=5)
                    env.assertFalse(thread.is_alive())
                    env.assertEqual(len(errors), 1, message=errors)
                    env.assertContains('Timeout limit was reached', str(errors[0]))
                    env.assertEqual(env.cmd(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', cleanup), 0)
                    env.assertTrue(client.ping())
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', serialized).ok()

                thread.join(timeout=5)
                env.assertFalse(thread.is_alive())
                env.assertEqual(results, [])
                env.assertEqual(len(errors), 1, message=errors)
                env.assertTrue(isinstance(errors[0], redis_exceptions.ResponseError), message=errors)
                if not cancel:
                    if stage == 'empty_fanout':
                        env.assertEqual(str(errors[0]), expected_error)
                    else:
                        env.assertContains(expected_error, str(errors[0]))
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', cleanup) == 1, {}),
                    'Failed search did not complete cleanup', timeout=5)
                env.assertTrue(client.ping())
            finally:
                if point:
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', serialized)
                thread.join(timeout=5)
                env.cmd(debug_cmd(), 'SEND_ERROR', 0)
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                client.close()
                pool.disconnect()
                if not dropped:
                    env.expect('FT.DROPINDEX', 'idx').ok()


@skip(cluster=False)
def test_background_search_errors_resp2():
    """RESP2 errors encode on workers and lose to an MT FAIL timeout."""
    _exercise_background_search_error(2)


@skip(cluster=False)
def test_background_search_errors_resp3():
    """RESP3 errors encode on workers and lose to an MT FAIL timeout."""
    _exercise_background_search_error(3)


@skip(cluster=False)
def test_background_search_deadline_during_serialization():
    """The real FAIL deadline remains active while the worker holds a partial reply."""
    _exercise_cleanup('serializing', real_timeout=True)


@skip(cluster=False)
def test_cancel_after_background_search_serialization_resp2():
    """A complete RESP2 reply is discarded when cancellation wins before unblocking."""
    _exercise_cleanup('serialized', protocol=2)


@skip(cluster=False)
def test_cancel_during_background_search_profile_serialization():
    """PROFILE serialization stays on the worker while FAIL cancellation replies on main."""
    _exercise_cleanup('serializing', profile=True)


@skip(cluster=False)
def test_background_search_profile_reply_resp2():
    """RESP2 PROFILE flushes its worker-buffered results and profile together."""
    _exercise_background_search_reply(2, profile=True)


@skip(cluster=False)
def test_background_search_profile_reply_resp3():
    """RESP3 PROFILE flushes its worker-buffered results and profile together."""
    _exercise_background_search_reply(3, profile=True)
