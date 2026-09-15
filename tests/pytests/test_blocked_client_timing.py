# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import struct
import threading
import time

from common import *
from test_blocked_client_timeout import TIMEOUT_WARNING, wait_for_blocked_query_client


def create_query_timing_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'v', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    vector = struct.pack('ff', 1, 2)
    getConnectionByEnv(env).execute_command('HSET', 'doc', 't', 'hello', 'v', vector)
    return vector


def assert_background_duration(env, command, sync_point, force_timeout, expected_reply=None,
                               expected_error=None, while_paused=None, command_name=None,
                               timeout_releases_worker=False):
    """Check elapsed work; sync_point=None pauses after storing results, before signalling."""
    if command_name is None:
        command_name = command[0] + ('|' + command[1] if command[0] == 'FT.CURSOR' else '')
    stat_key = 'cmdstat_' + command_name
    before = env.cmd('INFO', 'COMMANDSTATS').get(stat_key, {}).get('usec', 0)
    replies = []

    def run():
        try:
            with env.getConnection().client() as connection:
                if command[0].startswith('_'):
                    connection.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
                replies.append(connection.execute_command(*command))
        except Exception as error:
            replies.append(error)

    worker = threading.Thread(target=run, daemon=True)
    if sync_point is None:
        setPauseAfterStoreResults(env, True, internal=False)
        is_paused = lambda: getIsStoreResultsPaused(env)
    else:
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()
        is_paused = lambda: env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point)
    try:
        worker.start()
        wait_for_condition(
            lambda: (is_paused() == 1, {}),
            f'Worker did not pause at {sync_point or "stored results"}', timeout=10)
        client_id = wait_for_blocked_query_client(env, command_name, timeout=10)
        # The hook fixes the event order. This delay supplies a lower bound on
        # elapsed time; a slower host only increases the measured interval.
        time.sleep(0.05)
        if while_paused is not None:
            while_paused()
        if force_timeout:
            env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
        elif sync_point is None:
            resetStoreResultsDebug(env)
        else:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()
        worker.join(timeout=10)
        if force_timeout and not timeout_releases_worker:
            env.assertEqual(is_paused(), 1)
        env.assertFalse(worker.is_alive())
        env.assertEqual(len(replies), 1)
        if expected_reply is not None:
            env.assertEqual(replies[0], expected_reply)
        elif expected_error is not None:
            env.assertTrue(isinstance(replies[0], Exception), message=str(replies[0]))
            env.assertContains(expected_error, str(replies[0]))
        elif force_timeout:
            if isinstance(replies[0], Exception):
                env.assertContains('Timeout limit was reached', str(replies[0]))
            else:
                result = replies[0][0] if command[0] == 'FT.CURSOR' else replies[0]
                assert_timeout_warning(env, result, message=str(result))
        else:
            env.assertFalse(isinstance(replies[0], Exception), message=str(replies[0]))
        duration = env.cmd('INFO', 'COMMANDSTATS')[stat_key]['usec'] - before
        env.assertGreaterEqual(duration, 40000, message={'duration': duration, 'reply': replies})
    finally:
        if sync_point is None:
            resetStoreResultsDebug(env)
        else:
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        worker.join(timeout=10)

        def pool_is_idle(get_stats):
            stats = get_stats(env)
            return (stats['totalPendingJobs'] == 0 and stats['numJobsInProgress'] == 0, stats)

        # Timeout replies can precede loader cleanup, which still needs the GIL.
        wait_for_condition(lambda: pool_is_idle(getWorkersThpoolStats),
                           f'Workers did not finish {command_name}', timeout=10)
        if env.isCluster():
            wait_for_condition(lambda: pool_is_idle(getCoordThpoolStats),
                               f'Coordinator did not finish {command_name}', timeout=10)
    env.assertEqual(env.cmd('INFO', 'COMMANDSTATS')[stat_key]['usec'] - before, duration)
    return replies[0]


def assert_timeout_duration(env, command, sync_point):
    """Pin a running worker so the timeout must publish its elapsed interval."""
    return assert_background_duration(env, command, sync_point, force_timeout=True)


@skip(cluster=True)
def test_query_timeout_commits_background_duration():
    """FAIL and RETURN_STRICT must record query work before the worker resumes."""
    # Workers and RESP3 are required for the paused pipeline and warning reply.
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    vector = create_query_timing_index(env)
    commands = [
        (['FT.SEARCH', 'idx', '*'], 'BeforeSpecLock'),
        (['FT.AGGREGATE', 'idx', '*'], 'BeforeSpecLock'),
        (['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
          'PARAMS', '2', 'BLOB', vector], 'BeforeSpecLock'),
        (['_FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB', 'WITHCURSOR',
          '_SLOTS_INFO', generate_slots(), 'PARAMS', '2', 'BLOB', vector], 'BeforeSpecLock'),
    ]
    previous = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
    try:
        for policy in ('fail', 'return-strict'):
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy).ok()
            for command, hook in commands:
                assert_timeout_duration(env, command, hook)
    finally:
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous)


@skip(cluster=True)
def test_return_strict_loader_preemption_commits_background_duration():
    """A timeout after the results claim publishes work before preempting a safe loader."""
    # Worker hooks select the timeout order; RESP3 exposes the empty warning reply.
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0 ON_TIMEOUT RETURN-STRICT', protocol=3)
    skipIfNoEnableAssert(env)
    vector = create_query_timing_index(env)
    commands = (
        ['FT.SEARCH', 'idx', '*', 'RETURN', 1, 't'],
        ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t'],
        ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
         'PARAMS', '2', 'BLOB', vector, 'LOAD', 1, '@t'],
    )
    claim_hook = 'AfterHybridResultsClaim'

    def wait_for_hybrid_claim():
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', claim_hook) == 1, {}),
            'Hybrid tail did not claim results', timeout=10)

    for command in commands:
        hybrid = command[0] == 'FT.HYBRID'
        try:
            if hybrid:
                # A depleter can reach its loader before the tail claims results.
                # Holding both hooks forces the callback past its early-claim return.
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', claim_hook).ok()
                expected_reply = {
                    'total_results': 0, 'results': [], 'warnings': [TIMEOUT_WARNING],
                    'execution_time': 0.0,
                }
            else:
                expected_reply = {
                    'attributes': [], 'format': 'STRING', 'results': [],
                    'total_results': 0, 'warning': [TIMEOUT_WARNING],
                }
            assert_background_duration(
                env, command, 'AfterSafeLoaderGILHandshake', force_timeout=True,
                expected_reply=expected_reply, timeout_releases_worker=True,
                while_paused=wait_for_hybrid_claim if hybrid else None)
        finally:
            if hybrid:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', claim_hook)
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')


@skip(cluster=True)
def test_return_strict_stored_results_commits_background_duration():
    """Timeout callbacks that wait for stored results publish elapsed worker time once."""
    # Disable clock timeouts so the after-store hook always precedes CLIENT UNBLOCK.
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0 ON_TIMEOUT RETURN-STRICT', protocol=3)
    skipIfNoEnableAssert(env)
    vector = create_query_timing_index(env)
    commands = (
        ['FT.SEARCH', 'idx', '*', 'RETURN', 1, 't'],
        ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t'],
        ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
         'PARAMS', '2', 'BLOB', vector],
    )
    for command in commands:
        expected_reply = env.cmd(*command)
        if command[0] == 'FT.HYBRID':
            expected_reply['execution_time'] = ANY
        # Store completion follows the results claim and all loader GIL exits,
        # but the worker has not signalled the timeout callback yet.
        assert_background_duration(
            env, command, None, force_timeout=True, expected_reply=expected_reply,
            timeout_releases_worker=True)


@skip(cluster=True)
def test_query_completion_commits_background_duration():
    """Successful search, aggregate, and hybrid workers publish their elapsed interval."""
    # Disable timeouts so only worker completion can publish the paused interval.
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    vector = create_query_timing_index(env)
    commands = (
        ['FT.SEARCH', 'idx', '*', 'RETURN', 1, 't'],
        ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t'],
        ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
         'PARAMS', '2', 'BLOB', vector],
    )
    previous = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
    try:
        for policy in ('fail', 'return-strict', 'return'):
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy).ok()
            for command in commands:
                expected_reply = env.cmd(*command)
                if command[0] == 'FT.HYBRID':
                    expected_reply['execution_time'] = ANY
                assert_background_duration(
                    env, command, 'BeforeSpecLock', force_timeout=False,
                    expected_reply=expected_reply)
    finally:
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous)


@skip(cluster=True)
def test_hybrid_worker_error_commits_background_duration():
    """A hybrid worker that loses its index after pickup still publishes elapsed work."""
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    vector = struct.pack('ff', 1, 2)
    command = ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$BLOB',
               'PARAMS', '2', 'BLOB', vector]
    previous = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
    try:
        for policy in ('fail', 'return-strict', 'return'):
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy).ok()
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'v', 'VECTOR', 'FLAT', '6',
                       'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
            # This hybrid hook precedes index-reference promotion, so dropping
            # the index forces the worker's early error endpoint after timing starts.
            assert_background_duration(
                env, command, 'BeforeSpecLock', force_timeout=False,
                expected_error='The index was dropped before the query could be executed',
                while_paused=lambda: env.expect('FT.DROPINDEX', 'idx').ok())
    finally:
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous)


@skip(cluster=True)
def test_cursor_timeout_commits_background_duration():
    """Cursor timeout endpoints publish fresh intervals after a successful read."""
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(10):
        conn.execute_command('HSET', f'doc:{i}', 't', 'hello')
    previous = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
    try:
        for policy, hook in (
            ('fail', 'BeforeSpecLock'),
            ('return-strict', 'BeforeSpecLock'),
            ('return-strict', 'AfterSafeLoaderGILHandshake'),
            ('return-strict', None),
        ):
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy)
            _, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t',
                                'WITHCURSOR', 'COUNT', 1)
            env.assertNotEqual(cursor, 0)
            result, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 1)
            env.assertNotEqual(cursor, 0)
            expected_reply = None
            if policy == 'return-strict':
                # Identical documents make the stored chunk match the successful read.
                # Earlier timeout endpoints reply before consuming stored rows.
                if hook is not None:
                    result = {
                        'attributes': [], 'format': 'STRING', 'results': [],
                        'total_results': 0,
                    }
                result['warning'] = [TIMEOUT_WARNING]
                expected_reply = [result, 0]
            assert_background_duration(
                env, ['FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 1], hook,
                force_timeout=True, expected_reply=expected_reply,
                timeout_releases_worker=hook != 'BeforeSpecLock')
    finally:
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous)


def test_cursor_worker_error_commits_background_duration():
    """A cursor read that loses its index publishes its worker duration exactly once."""
    # Disable timeouts so the dropped-index worker endpoint finalizes the interval.
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    conn = getConnectionByEnv(env)
    previous = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
    try:
        for policy in ('fail', 'return-strict', 'return'):
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy).ok()
            index = f'cursor-error-{policy}'
            prefix = f'{{{index}}}:'
            env.expect('FT.CREATE', index, 'PREFIX', 1, prefix,
                       'SCHEMA', 't', 'TEXT').ok()
            for i in range(10):
                conn.execute_command('HSET', f'{prefix}{i}', 't', 'hello')
            _, cursor = env.cmd('FT.AGGREGATE', index, '*', 'WITHCURSOR', 'COUNT', 1)
            env.assertNotEqual(cursor, 0)

            # The cursor hook precedes index-reference promotion, so the resumed
            # worker must take the dropped-index error path after timing starts.
            assert_background_duration(
                env, ['FT.CURSOR', 'READ', index, cursor, 'COUNT', 1],
                'BeforeSpecLock', force_timeout=False,
                expected_error='The index was dropped while the cursor was idle',
                while_paused=lambda: env.expect('FT.DROPINDEX', index).ok())
    finally:
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous)


def test_cursor_completion_commits_background_duration():
    """Successful cursor reads publish fresh worker intervals under every policy."""
    # Disable timeouts so only the worker can finish each measured interval.
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(10):
        conn.execute_command('HSET', f'doc:{i}', 't', 'hello')
    expected_result = {
        'attributes': [], 'warning': [], 'total_results': 0, 'format': 'STRING',
        'results': [{'extra_attributes': {'t': 'hello'}, 'values': []}],
    }
    previous = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
    try:
        for policy in ('fail', 'return-strict', 'return'):
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy).ok()
            _, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'SORTBY', 1, '@t',
                                'WITHCURSOR', 'COUNT', 1)
            env.assertNotEqual(cursor, 0)
            try:
                for _ in range(2):
                    reply = assert_background_duration(
                        env, ['FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 1],
                        'BeforeSpecLock', force_timeout=False)
                    env.assertEqual(reply, [expected_result, cursor])
            finally:
                env.expect('FT.CURSOR', 'DEL', 'idx', cursor).ok()
    finally:
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous)


@skip(cluster=False)
def test_coordinator_cursor_timeout_commits_background_duration():
    """Coordinator cursor callbacks commit their worker interval under both timeout policies."""
    env = Env(moduleArgs='WORKERS 2 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(10):
        conn.execute_command('HSET', f'{{doc}}:{i}', 't', 'hello')
    previous = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
    try:
        for policy, hook in (
            ('fail', 'BeforeSpecLock'),
            ('return-strict', 'BeforeSpecLock'),
            ('return-strict', 'BeforeCoordCursorReadFinish'),
        ):
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy)
            _, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 1)
            env.assertNotEqual(cursor, 0)
            _, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 1)
            env.assertNotEqual(cursor, 0)
            assert_timeout_duration(env, ['FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 1],
                                    hook)
    finally:
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous)
