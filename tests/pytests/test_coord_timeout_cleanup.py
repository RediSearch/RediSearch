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
from test_blocked_client_timeout import is_client_blocked


def _exercise_cleanup(stage, debug_query=False, protocol=3):
    # Force each cancellation at a particular coordinator ownership transition.
    # GC retains its own weak reference until its next timer, even after DROPINDEX.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0 NOGC', protocol=protocol)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    points = {
        'prepare': 'BeforeCoordSearchPrepare',
        'fanout': 'BeforeCoordFanout',
        'empty_fanout': 'BeforeCoordFanout',
        'reducer_claim': 'BeforeCoordReducerClaim',
        'serialized': 'CoordSearchReplySerialized',
        'serializing': 'CoordSearchReplyStarted',
    }
    point = points.get(stage)
    cleanup_point = 'CoordSearchFreePrivData'
    policies = ('return',) if debug_query else ('return', 'fail')
    if stage == 'serialized':
        policies = ('return', 'fail')
    elif stage == 'serializing':
        policies = ('fail',)
    for policy in policies:
        cancellations = ('disconnect',) if policy == 'return' else ('timeout', 'disconnect')
        for cancellation in cancellations:
            env.expect('CONFIG', 'SET', 'search-on-timeout', policy).ok()
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
            getConnectionByEnv(env).execute_command('HSET', '{doc}:1', 'name', 'hello')
            expected = env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT')
            original = env.getConnection().connection_pool
            kwargs = dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0))
            pool = ConnectionPool(connection_class=original.connection_class, **kwargs)
            client = Redis(connection_pool=pool, single_connection_client=True)
            client_id = client.client_id()
            results, errors = [], []

            command = ['FT.SEARCH', 'idx', '*', 'NOCONTENT']
            if debug_query:
                command = [debug_cmd(), *command, 'TIMEOUT_AFTER_N', 1000, 'DEBUG_PARAMS_COUNT', 2]

            def query():
                try:
                    results.append(client.execute_command(*command))
                except Exception as error:
                    errors.append(error)

            # The callback runs on main, so the observation point must self-release.
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', cleanup_point, 1).ok()
            if stage in ('queued', 'prepare'):
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
                    env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                else:
                    env.expect('CLIENT', 'KILL', 'ID', client_id).equal(1)
                thread.join(timeout=5)
                env.assertFalse(thread.is_alive())
                if cancellation == 'disconnect':
                    env.assertEqual(results, [])
                    env.assertEqual(len(errors), 1)
                    env.assertTrue(isinstance(errors[0], redis_exceptions.ConnectionError),
                                   message=errors)
                elif policy == 'fail':
                    env.assertEqual(results, [])
                    env.assertEqual(len(errors), 1)
                    env.assertContains('Timeout limit was reached', str(errors[0]))
                else:
                    env.assertEqual(errors, [])
                    env.assertEqual(len(results), 1)
                    expected_rows = expected['results'] if stage in ('reducer_claim', 'reduce') else []
                    env.assertEqual(results[0]['results'], expected_rows)
                    env.assertEqual(results[0]['warning'], ['Timeout limit was reached'])
                if stage == 'prepare':
                    # Preparation now fails after the timeout already owns the reply.
                    env.expect('FT.DROPINDEX', 'idx').ok()
                elif stage == 'empty_fanout':
                    env.expect(debug_cmd(), 'SEND_ERROR', env.shardsCount).ok()
                elif stage in ('serialized', 'serializing'):
                    env.assertTrue(env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point))
                    env.assertEqual(env.cmd(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', cleanup_point), 0)
                    if cancellation == 'timeout':
                        env.assertTrue(client.ping())
                if stage == 'queued':
                    env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
                    coord_paused = False
                elif stage == 'reduce' and cancellation == 'disconnect':
                    # These branches do not cancel coordinator work on disconnect.
                    setCoordReduceResume(env)
                elif point:
                    env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', cleanup_point) == 1, {}),
                    'Redis never freed the completed blocked query', timeout=5)
                env.assertTrue(env.cmd('PING'))
                if stage in ('serialized', 'serializing') and cancellation == 'timeout':
                    env.assertTrue(client.ping())
            finally:
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
                    if stage in ('queued', 'prepare'):
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
def test_timeout_cleanup_fanout():
    """The last shard reply must finish a cancelled fanout."""
    _exercise_cleanup('fanout')


@skip(cluster=False)
def test_timeout_cleanup_empty_fanout():
    """A cancelled fanout with no sent commands has no future reply to finish it."""
    _exercise_cleanup('empty_fanout')


@skip(cluster=False)
def test_timeout_cleanup_reducer_claim():
    """A worker that loses reduction ownership still owes blocked completion."""
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
def test_cancel_after_background_search_serialization():
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


def _exercise_background_search_reply(protocol):
    # Both RESP versions must flush the BG buffer even with the timer disabled.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0', protocol=protocol)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    getConnectionByEnv(env).execute_command('HSET', '{doc}:1', 'name', 'hello')
    point = 'CoordSearchReplySerialized'
    for policy in ('fail', 'return'):
        env.expect('CONFIG', 'SET', 'search-on-timeout', policy).ok()
        for timeout in (0, 60000):
            command = ['FT.SEARCH', 'idx', '*', 'NOCONTENT', 'TIMEOUT', timeout]
            expected = env.cmd(*command)
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
    # The same worker error must stay on MT for strict and move to BG otherwise.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0 NOGC', protocol=protocol)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    serialized = 'CoordSearchReplySerialized'
    cleanup = 'CoordSearchFreePrivData'
    cases = (
        ('shard', None, '@name:(', 'Syntax error'),
        ('prepare', 'BeforeCoordSearchPrepare', '*', 'index was dropped'),
        ('empty_fanout', 'BeforeCoordFanout', '*', 'Could not send query to cluster'),
    )
    for stage, point, query, expected_error in cases:
        for policy, cancel in (('fail', False), ('return', False),
                               ('return-strict', False), ('fail', True)):
            if stage == 'empty_fanout' and cancel:
                continue  # Cancellation before zero-send fanout is covered by the cleanup test.
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

                if policy != 'return-strict' and stage != 'empty_fanout':
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
                    env.assertContains(expected_error, str(errors[0]))
                if policy == 'return-strict' or stage == 'empty_fanout':
                    env.assertEqual(env.cmd(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', serialized), 0)
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
    """RESP2 errors encode on the policy's owner and lose to an MT FAIL timeout."""
    _exercise_background_search_error(2)


@skip(cluster=False)
def test_background_search_errors_resp3():
    """RESP3 errors encode on the policy's owner and lose to an MT FAIL timeout."""
    _exercise_background_search_error(3)
