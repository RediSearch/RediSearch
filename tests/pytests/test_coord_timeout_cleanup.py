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


def _exercise_cleanup(stage, debug_query=False, hold_worker=False):
    # Force each cancellation at a particular coordinator ownership transition.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    points = {
        'prepare': 'BeforeCoordSearchPrepare',
        'fanout': 'BeforeCoordFanout',
        'empty_fanout': 'BeforeCoordFanout',
        'reducer_claim': 'BeforeCoordReducerClaim',
        'claimed': 'CoordSearchReducerClaimed',
    }
    point = points.get(stage)
    cleanup_point = 'CoordSearchFreePrivData'
    worker_done_point = 'CoordSearchWorkerDone'
    policies = ('return',) if debug_query else ('return', 'fail', 'return-strict')
    if stage == 'claimed':
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
            if hold_worker:
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', worker_done_point).ok()
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
                if stage == 'claimed':
                    env.expect(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point).equal(True)
                    env.expect(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', cleanup_point).equal(0)
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
                if hold_worker:
                    wait_for_condition(
                        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', worker_done_point), {}),
                        'Worker did not retain its transport reference through request cleanup', timeout=5)
                env.assertTrue(env.cmd('PING'))
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
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                env.cmd(debug_cmd(), 'SEND_ERROR', 0)
                client.close()
                pool.disconnect()
                if stage != 'prepare':
                    env.expect('FT.DROPINDEX', 'idx').ok()
                getConnectionByEnv(env).execute_command('DEL', '{doc}:1')


@skip(cluster=False)
def test_timeout_cleanup_before_dispatch():
    """A queued query must finish its blocked handle after cancellation."""
    _exercise_cleanup('queued')


@skip(cluster=False)
def test_timeout_cleanup_prepare_error():
    """Preparation failure after cancellation must still finish the handle."""
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
    """The debug SEARCH entry must also complete a cancelled queued query."""
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
