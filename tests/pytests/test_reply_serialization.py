import threading
from struct import pack
from redis import ConnectionPool, Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from common import *


def _query_client(env):
    # A killed command must fail once, rather than reconnect and execute again
    # behind the same armed synchronization point.
    original = env.getConnection().connection_pool
    kwargs = dict(original.connection_kwargs)
    kwargs['retry'] = Retry(NoBackoff(), 0)
    pool = ConnectionPool(connection_class=original.connection_class, **kwargs)
    return Redis(connection_pool=pool, single_connection_client=True)


def _exercise_serialization(protocol):
    # Workers and both protocols are essential to the blocked reply-buffer contract.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0', protocol=protocol)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
               'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(2):
        conn.execute_command('HSET', f'doc:{i}', 'name', f'hello{i}', 'v', pack('ff', i, i))

    queries = [
        (['FT.SEARCH', 'idx', '*', 'SORTBY', 'name', 'RETURN', 1, 'name'],
         'DuringCoordRowSerialization' if env.isCluster() else 'DuringRowSerialization'),
        (['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name', 'SORTBY', 2, '@name', 'ASC'],
         'DuringRowSerialization'),
        (['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$vec',
          'LOAD', 1, '@name', 'PARAMS', 2, 'vec', pack('ff', 0, 0)],
         'DuringHybridRowSerialization'),
    ]
    previous = to_dict(env.cmd('CONFIG', 'GET', 'search-on-timeout'))['search-on-timeout']
    try:
        for policy in ('return', 'fail'):
            run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', policy)
            for args, point in queries:
                def normalize(result):
                    if args[0] == 'FT.HYBRID':
                        result = to_dict(result)
                        env.assertGreaterEqual(float(result.pop('execution_time')), 0)
                    return result

                expected = normalize(env.cmd(*args))
                actions = ('success', 'disconnect', 'timeout') if policy == 'fail' else ('success', 'disconnect')
                if env.isCluster() and args[0] == 'FT.SEARCH' and policy == 'fail':
                    # FAIL keeps its main-thread serializer to avoid extending the timeout wait.
                    continue
                for action in actions:
                    # A dedicated connection lets PING detect any second, leaked reply.
                    client = _query_client(env)
                    client_id = client.client_id()
                    results, errors = [], []

                    def execute():
                        try:
                            results.append(normalize(client.execute_command(*args)))
                        except Exception as error:
                            errors.append(error)

                    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
                    thread = threading.Thread(target=execute)
                    thread.start()
                    try:
                        wait_for_condition(
                            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
                            f'serialization did not reach {point}', timeout=5)
                        env.assertTrue(env.cmd('PING'))
                        if action != 'success':
                            if action == 'timeout':
                                env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                            else:
                                env.expect('CLIENT', 'KILL', 'ID', client_id).equal(1)
                            thread.join(timeout=5)
                            env.assertFalse(thread.is_alive())
                            env.assertEqual(results, [])
                            env.assertEqual(len(errors), 1)
                            if action == 'timeout':
                                env.assertContains('Timeout limit was reached', str(errors[0]))
                            else:
                                env.assertTrue(isinstance(errors[0], redis_exceptions.ConnectionError), message=errors)
                            env.assertTrue(client.ping())
                    finally:
                        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                        thread.join(timeout=5)
                        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                    env.assertFalse(thread.is_alive())
                    if action == 'success':
                        env.assertEqual(errors, [])
                        env.assertEqual(results, [expected])
                    env.assertTrue(client.ping())
                    client.close()
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', previous)


def test_worker_reply_serialization_resp2():
    """Main remains available during serialization; FAIL discards a partial RESP2 reply."""
    _exercise_serialization(2)


def test_worker_reply_serialization_resp3():
    """Main remains available during serialization; FAIL discards a partial RESP3 reply."""
    _exercise_serialization(3)


def test_cursor_worker_serialization():
    """Each cursor chunk is encoded on a worker, including after a callback parks it."""
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0', protocol=2)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(3):
        conn.execute_command('HSET', f'doc:{i}', 'n', i)
    previous = to_dict(env.cmd('CONFIG', 'GET', 'search-on-timeout'))['search-on-timeout']
    try:
        for policy in ('return', 'fail'):
            run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', policy)
            args = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                    'SORTBY', 2, '@n', 'ASC', 'WITHCURSOR', 'COUNT', 1]
            rows = []
            for i in range(3):
                results, errors = [], []

                def execute():
                    try:
                        results.append(env.cmd(*args))
                    except Exception as error:
                        errors.append(error)

                env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', 'DuringRowSerialization')
                thread = threading.Thread(target=execute)
                thread.start()
                try:
                    wait_for_condition(
                        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING',
                                         'DuringRowSerialization'), {}),
                        'cursor chunk did not serialize on worker', timeout=5)
                    env.assertTrue(env.cmd('PING'))
                finally:
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', 'DuringRowSerialization')
                    thread.join(timeout=5)
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                env.assertFalse(thread.is_alive())
                env.assertEqual(errors, [])
                chunk, cursor = results[0]
                env.assertEqual(chunk, [3 if i == 0 else 0, ['n', str(i)]])
                rows.extend(chunk[1:])
                env.assertNotEqual(cursor, 0)
                args = ['FT.CURSOR', 'READ', 'idx', cursor]
            env.assertEqual(rows, [['n', '0'], ['n', '1'], ['n', '2']])
            env.assertEqual(env.cmd(*args), [[0], 0])
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', previous)


@skip(cluster=False)
def test_fail_timeout_counted_once_before_serialization():
    """A pipeline noticing an already-replied timeout must not count it again."""
    from test_info_modules import (info_modules_to_dict, COORD_WARN_ERR_SECTION,
                                   TIMEOUT_ERROR_COORD_METRIC)

    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL', protocol=3)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    baseline = int(info_modules_to_dict(env)[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
    client = _query_client(env)
    client_id = client.client_id()
    errors = []

    def execute():
        try:
            client.execute_command('FT.AGGREGATE', 'idx', '*')
        except Exception as error:
            errors.append(error)

    jobs_before = to_dict(env.cmd(debug_cmd(), 'COORD_THREADS', 'STATS'))['totalJobsDone']
    point = 'BeforeRPNetStart'
    env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', point)
    thread = threading.Thread(target=execute)
    thread.start()
    try:
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
            'pipeline did not reach RPNet start', timeout=5)
        env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
        thread.join(timeout=5)
        env.assertFalse(thread.is_alive())
        env.assertEqual(len(errors), 1)
        env.assertContains('Timeout limit was reached', str(errors[0]))
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
        thread.join(timeout=5)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        client.close()
    wait_for_condition(
        lambda: (to_dict(env.cmd(debug_cmd(), 'COORD_THREADS', 'STATS'))['totalJobsDone'] > jobs_before, {}),
        'timed-out worker did not finish', timeout=5)
    after = int(info_modules_to_dict(env)[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
    env.assertEqual(after, baseline + 1)


@skip(cluster=False)
def test_coordinator_serialization_policy_is_fixed_when_blocked():
    """Changing ON_TIMEOUT cannot change who owns an outstanding request's reply."""
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0', protocol=2)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    getConnectionByEnv(env).execute_command('HSET', 'doc', 'name', 'hello')
    previous = to_dict(env.cmd('CONFIG', 'GET', 'search-on-timeout'))['search-on-timeout']
    point = 'BeforeCoordWorkerSerialization'
    try:
        for initial, changed in (('return', 'fail'), ('fail', 'return'),
                                 ('return', 'return-strict'), ('return-strict', 'return')):
            env.expect('CONFIG', 'SET', 'search-on-timeout', initial).ok()
            setPauseBeforeReduce(env, PAUSE_BEFORE_REDUCER_INIT)
            results, errors = [], []

            def execute():
                try:
                    results.append(env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT'))
                except Exception as error:
                    errors.append(error)

            thread = threading.Thread(target=execute)
            thread.start()
            try:
                wait_for_condition(lambda: (getIsCoordReducePaused(env), {}),
                                   'reducer did not pause before initialization', timeout=5)
                env.expect('CONFIG', 'SET', 'search-on-timeout', changed).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
                setCoordReduceResume(env)
                if initial == 'return':
                    wait_for_condition(
                        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
                        'RETURN request stopped serializing on its worker after CONFIG SET', timeout=5)
                    env.assertTrue(env.cmd('PING'))
                else:
                    thread.join(timeout=5)
                    env.assertFalse(thread.is_alive(), message=f'{initial} reply moved onto the worker')
            finally:
                resetCoordReduceDebug(env)
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=5)
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            env.assertFalse(thread.is_alive())
            env.assertEqual(errors, [])
            env.assertEqual(results, [[1, 'doc']])
    finally:
        resetCoordReduceDebug(env)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.expect('CONFIG', 'SET', 'search-on-timeout', previous).ok()
