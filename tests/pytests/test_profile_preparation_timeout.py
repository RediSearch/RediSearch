from common import *
import threading

from test_blocked_client_timeout import _wait_for_background_fail_workers
from test_info_modules import (
    info_modules_to_dict, COORD_WARN_ERR_SECTION,
    TIMEOUT_ERROR_COORD_METRIC, TIMEOUT_WARNING_COORD_METRIC,
    MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC,
)


def _profile_command(kind, timeout=0, invalid_pipeline=False):
    command = ['FT.PROFILE', 'idx', kind, 'QUERY', '*', 'TIMEOUT', timeout]
    if kind == 'SEARCH':
        command += ['NOCONTENT']
    elif invalid_pipeline:
        command += ['LOAD', 2, '@name', 'AS']
    else:
        command += ['LOAD', 1, '@name']
    return command


def _assert_timeout_profile(env, response, protocol, pipeline_ready, prefix_warning=False):
    warnings = ['Timeout limit was reached']
    if prefix_warning:
        warnings.append('Max prefix expansions limit was reached')
    if protocol == 3:
        env.assertEqual(response['Results']['results'], [], message=response)
        env.assertEqual(response['Results']['total_results'], 0, message=response)
        env.assertEqual(response['Results']['warning'], warnings, message=response)
        profile = response['Profile']
    else:
        env.assertEqual(response[0], [0], message=response)
        profile = to_dict(response[1])
    env.assertEqual(len(profile['Shards']), 1, message=response)
    shard = profile['Shards'][0]
    if protocol == 2:
        shard = to_dict(shard)
    env.assertEqual(shard['Warning'], warnings, message=response)
    for field in ('Total profile time', 'Parsing time', 'Workers queue time',
                  'Pipeline creation time'):
        env.assertGreaterEqual(float(shard[field]), 0, message=response)
    processors = shard['Result processors profile']
    if pipeline_ready:
        env.assertGreater(len(processors), 0, message=response)
        env.assertTrue('Iterators profile' in shard, message=response)
    for processor in processors:
        if protocol == 2:
            processor = to_dict(processor)
        env.assertEqual(processor['Results processed'], 0, message=response)


def _exercise_preparation_timeout(point, invalid_pipeline=False, automatic=False, prefix_warning=False):
    for protocol in (2, 3):
        # A real worker and both protocols exercise the timeout/worker handoff.
        env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC')
        skipIfNoEnableAssert(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
        conn = getConnectionByEnv(env)
        conn.execute_command('HSET', 'doc', 'name', 'hello')
        if prefix_warning:
            env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', 1).ok()
            conn.execute_command('HSET', 'doc:2', 'name', 'help')
        kinds = ('AGGREGATE',) if invalid_pipeline else ('SEARCH', 'AGGREGATE')
        for kind in kinds:
            client = env.getConnection()
            connection = client.connection_pool.get_connection()
            connection.send_command('CLIENT', 'ID')
            client_id = connection.read_response()
            replies, errors = [], []

            def query():
                try:
                    command = _profile_command(kind, 1000 if automatic else 0, invalid_pipeline)
                    if prefix_warning:
                        command[4] = 'hel*'
                    connection.send_command(*command)
                    replies.append(connection.read_response())
                except Exception as error:
                    errors.append(error)

            before = info_modules_to_dict(env)[COORD_WARN_ERR_SECTION]
            thread = threading.Thread(target=query, daemon=True)
            if point == 'queued':
                env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
            else:
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
            try:
                thread.start()
                if point == 'queued':
                    wait_for_condition(
                        lambda: (to_dict(env.cmd(debug_cmd(), 'WORKERS', 'STATS'))[
                            'totalPendingJobs'] > 0, {'replies': replies, 'errors': errors}),
                        'Profile did not enter the worker queue', timeout=5)
                else:
                    wait_for_condition(
                        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
                        f'Profile did not reach {point}', timeout=5)
                if not automatic:
                    env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                thread.join(timeout=5)
                env.assertFalse(thread.is_alive(), message=f'{kind}: {point}')
                env.assertEqual(errors, [])
                env.assertEqual(len(replies), 1, message=replies)
                _assert_timeout_profile(env, replies[0], protocol, point == 'AfterIteratorCreate',
                                        prefix_warning)
                connection.send_command('PING')
                env.assertEqual(connection.read_response(), 'PONG')
                after = info_modules_to_dict(env)[COORD_WARN_ERR_SECTION]
                env.assertEqual(int(after[TIMEOUT_WARNING_COORD_METRIC]),
                                int(before[TIMEOUT_WARNING_COORD_METRIC]) + 1)
                env.assertEqual(after[TIMEOUT_ERROR_COORD_METRIC], before[TIMEOUT_ERROR_COORD_METRIC])
                env.assertEqual(int(after[MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC]),
                                int(before[MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC]) + int(prefix_warning))
            finally:
                if point == 'queued':
                    env.cmd(debug_cmd(), 'WORKERS', 'RESUME')
                else:
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=5)
                client.connection_pool.release(connection)
            _wait_for_background_fail_workers(env)
        if prefix_warning:
            env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', 200).ok()


@skip(cluster=True)
def test_profile_fail_timeout_while_queued():
    """A timeout claims an unstarted job and replies without waiting for a worker."""
    _exercise_preparation_timeout('queued')


@skip(cluster=True)
def test_profile_fail_deadline_while_queued():
    """The automatic blocked-client deadline uses the same profile reply as explicit timeout."""
    _exercise_preparation_timeout('queued', automatic=True)


@skip(cluster=True)
def test_profile_fail_timeout_before_preparation():
    """The timeout waits for a started worker to publish safe preparation state."""
    _exercise_preparation_timeout('BeforeSpecLock')


@skip(cluster=True)
def test_profile_fail_timeout_after_preparation():
    """Completed pipeline structure is reported with zero processed rows and no counter underflow."""
    _exercise_preparation_timeout('AfterIteratorCreate')


@skip(cluster=True)
def test_profile_fail_timeout_preserves_preparation_warnings():
    """Warnings raised while constructing iterators remain in the timeout profile."""
    _exercise_preparation_timeout('AfterIteratorCreate', prefix_warning=True)


@skip(cluster=True)
def test_profile_fail_timeout_with_preparation_error():
    """A failed pipeline build must acknowledge the timeout without printing a partial RP chain."""
    _exercise_preparation_timeout('ProfileBeforePipelineBuild', invalid_pipeline=True)


@skip(cluster=True)
def test_profile_fail_index_dropped_while_queued():
    """A dropped index completes preparation ownership and frees the profile synchronization state."""
    for protocol in (2, 3):
        env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC')
        skipIfNoEnableAssert(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
        errors = []

        def query():
            try:
                response = env.cmd(*_profile_command('SEARCH'))
                errors.append(AssertionError(f'Expected dropped-index error, got {response}'))
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=query, daemon=True)
        env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
        try:
            thread.start()
            wait_for_condition(
                lambda: (to_dict(env.cmd(debug_cmd(), 'WORKERS', 'STATS'))['totalPendingJobs'] > 0, {}),
                'Profile did not enter the worker queue', timeout=5)
            env.expect('FT.DROPINDEX', 'idx').ok()
        finally:
            env.cmd(debug_cmd(), 'WORKERS', 'RESUME')
            thread.join(timeout=5)
        env.assertFalse(thread.is_alive())
        env.assertEqual(len(errors), 1, message=errors)
        env.assertTrue(isinstance(errors[0], redis_exceptions.ResponseError), message=errors)
        env.assertContains('dropped', str(errors[0]))
        _wait_for_background_fail_workers(env)
        env.assertTrue(env.cmd('PING'))


@skip(cluster=True)
def test_profile_fail_after_execution_starts():
    """This stack layer preserves the existing bare timeout error after execution is claimed."""
    for protocol in (2, 3):
        env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC')
        skipIfNoEnableAssert(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
        getConnectionByEnv(env).execute_command('HSET', 'doc', 'name', 'hello')
        for kind in ('SEARCH', 'AGGREGATE'):
            client = env.getConnection()
            connection = client.connection_pool.get_connection()
            connection.send_command('CLIENT', 'ID')
            client_id = connection.read_response()
            errors = []

            def query():
                try:
                    connection.send_command(*_profile_command(kind))
                    response = connection.read_response()
                    errors.append(AssertionError(f'Expected timeout error, got {response}'))
                except Exception as error:
                    errors.append(error)

            thread = threading.Thread(target=query, daemon=True)
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'BeforeFirstRead').ok()
            try:
                thread.start()
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', 'BeforeFirstRead') == 1, {}),
                    'Profile did not enter execution', timeout=5)
                env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                thread.join(timeout=5)
                env.assertFalse(thread.is_alive())
                env.assertEqual(len(errors), 1, message=errors)
                env.assertTrue(isinstance(errors[0], redis_exceptions.ResponseError), message=errors)
                env.assertContains('Timeout limit was reached', str(errors[0]))
            finally:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', 'BeforeFirstRead')
                thread.join(timeout=5)
                client.connection_pool.release(connection)
            _wait_for_background_fail_workers(env)
            # The same worker must still execute an ordinary successful profile.
            response = env.cmd(*_profile_command(kind))
            result = response['Results'] if protocol == 3 else response[0]
            env.assertEqual(result['total_results'] if protocol == 3 else result[0], 1,
                            message=response)
