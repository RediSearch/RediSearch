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
from test_hybrid_internal import get_shard_slot_ranges


def _exercise(protocol, cancel):
    # One worker and no clock deadline make the serialization race deterministic.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC', protocol=protocol)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    writer = getConnectionByEnv(env)
    for i in range(60):
        writer.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')
    control = env.getConnection(1) if env.isCluster() else env.getConnection()
    prefix = '_FT.' if env.isCluster() else 'FT.'
    extra = ['_SLOTS_INFO', get_shard_slot_ranges(env)[0][1],
             '_COORD_DISPATCH_TIME', '0'] if env.isCluster() else []
    commands = [
        [prefix + 'SEARCH', 'idx', '*', 'NOCONTENT'],
        [prefix + 'AGGREGATE', 'idx', '*', 'LOAD', '1', '@name'],
        [prefix + 'PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'NOCONTENT'],
        [prefix + 'PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', '1', '@name'],
    ]
    commands = [command + ['TIMEOUT', timeout] for command in commands
                for timeout in ((0,) if cancel else (0, 60000))]
    points = ('ShardReplyStarted', 'ShardReplySerialized') if cancel else ('ShardReplySerialized',)
    for command in commands:
        for point in points:
            for action in (('timeout', 'disconnect') if cancel else ('success',)):
                original = control.connection_pool
                pool = ConnectionPool(connection_class=original.connection_class,
                                      **dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0)))
                client = Redis(connection_pool=pool, single_connection_client=True)
                if env.isCluster():
                    client.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
                client_id = client.client_id()
                results, errors = [], []
                expected = client.execute_command(*command, *extra)
                control.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', point)
                control.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', 'ShardReplyFreed', 1)

                def execute():
                    try:
                        results.append(client.execute_command(*command, *extra))
                    except Exception as error:
                        errors.append(error)

                thread = threading.Thread(target=execute, daemon=True)
                try:
                    thread.start()
                    wait_for_condition(
                        lambda: (control.execute_command(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
                        f'Worker did not reach {point}: {command}', timeout=5)
                    env.assertEqual(results, [])
                    env.assertEqual(errors, [])
                    if action == 'timeout':
                        env.assertEqual(control.execute_command('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT'), 1)
                    elif action == 'disconnect':
                        env.assertEqual(control.execute_command('CLIENT', 'KILL', 'ID', client_id), 1)
                    if cancel:
                        thread.join(timeout=5)
                        env.assertFalse(thread.is_alive())
                        env.assertEqual(results, [])
                        env.assertEqual(len(errors), 1, message=errors)
                        if action == 'timeout':
                            env.assertTrue(isinstance(errors[0], redis_exceptions.ResponseError), message=errors)
                            env.assertContains('Timeout limit was reached', str(errors[0]))
                            env.assertTrue(client.ping())
                        else:
                            env.assertTrue(isinstance(errors[0], redis_exceptions.ConnectionError), message=errors)
                        env.assertEqual(control.execute_command(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT',
                                                                'ShardReplyFreed'), 0)
                    control.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                    thread.join(timeout=5)
                    env.assertFalse(thread.is_alive())
                    wait_for_condition(
                        lambda: (control.execute_command(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT',
                                                         'ShardReplyFreed') == 1, {}),
                        'Worker did not finish cleanup', timeout=5)
                    if not cancel:
                        env.assertEqual(errors, [])
                        env.assertEqual(len(results), 1)
                        actual = results[0]
                        if command[0].endswith('PROFILE'):
                            actual = actual['Results'] if protocol == 3 else actual[0]
                            expected = expected['Results'] if protocol == 3 else expected[0]
                        env.assertEqual(actual, expected)
                    if action != 'disconnect':
                        env.assertTrue(client.ping())
                finally:
                    control.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                    thread.join(timeout=5)
                    control.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                    client.close()
                    pool.disconnect()


def test_background_reply_resp2():
    """SEARCH/AGGREGATE and PROFILE flush BG replies with the timer disabled."""
    _exercise(2, False)


def test_background_reply_resp3():
    """RESP3 envelopes remain intact when the normal callback does not encode them."""
    _exercise(3, False)


def test_cancel_background_reply_resp2():
    """Timeout/disconnect during and after encoding discards bytes and preserves ownership."""
    _exercise(2, True)


def test_cancel_background_reply_resp3():
    """RESP3 partial and complete buffers never leak into the timeout reply."""
    _exercise(3, True)


def _exercise_cursor(protocol):
    # Cursor publication must happen only after Redis accepts the serialized reply.
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC', protocol=protocol)
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    writer = getConnectionByEnv(env)
    for i in range(60):
        writer.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')
    control = env.getConnection(1) if env.isCluster() else env.getConnection()
    prefix = '_FT.' if env.isCluster() else 'FT.'
    extra = ['_SLOTS_INFO', get_shard_slot_ranges(env)[0][1],
             '_COORD_DISPATCH_TIME', '0'] if env.isCluster() else []
    initial = [prefix + 'AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
               'WITHCURSOR', 'COUNT', 1, *extra]
    for read in (False, True):
        for action in ('success', 'timeout', 'disconnect'):
            for point in ('ShardReplyStarted', 'ShardReplySerialized'):
                original = control.connection_pool
                pool = ConnectionPool(connection_class=original.connection_class,
                                      **dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0)))
                client = Redis(connection_pool=pool, single_connection_client=True)
                if env.isCluster():
                    client.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
                    control.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
                client_id = client.client_id()
                cursor_id = 0
                if read:
                    _, cursor_id = client.execute_command(*initial)
                    env.assertNotEqual(cursor_id, 0)
                command = [prefix + 'CURSOR', 'READ', 'idx', cursor_id, 'COUNT', 1] if read else initial
                results, errors = [], []
                control.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', point)
                control.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', 'ShardReplyFreed', 1)

                def execute():
                    try:
                        results.append(client.execute_command(*command))
                    except Exception as error:
                        errors.append(error)

                def cursor_count():
                    info = control.execute_command(prefix + 'INFO', 'idx')
                    info = info if isinstance(info, dict) else to_dict(info)
                    stats = info['cursor_stats']
                    stats = stats if isinstance(stats, dict) else to_dict(stats)
                    return stats['global_total']

                thread = threading.Thread(target=execute, daemon=True)
                try:
                    thread.start()
                    wait_for_condition(
                        lambda: (control.execute_command(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
                        'Cursor did not serialize on BG', timeout=5)
                    if action != 'success':
                        if action == 'timeout':
                            env.assertEqual(control.execute_command('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT'), 1)
                        else:
                            env.assertEqual(control.execute_command('CLIENT', 'KILL', 'ID', client_id), 1)
                        thread.join(timeout=5)
                        env.assertFalse(thread.is_alive())
                        env.assertEqual(results, [])
                        env.assertEqual(len(errors), 1, message=errors)
                        if action == 'timeout':
                            env.assertContains('Timeout limit was reached', str(errors[0]))
                            env.assertTrue(client.ping())
                        else:
                            env.assertTrue(isinstance(errors[0], redis_exceptions.ConnectionError), message=errors)
                        env.assertEqual(control.execute_command(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT',
                                                                'ShardReplyFreed'), 0)
                    control.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                    thread.join(timeout=5)
                    env.assertFalse(thread.is_alive())
                    wait_for_condition(
                        lambda: (control.execute_command(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT',
                                                         'ShardReplyFreed') == 1, {}),
                        'Cursor worker did not finish', timeout=5)
                    if action == 'success':
                        env.assertEqual(errors, [])
                        env.assertEqual(len(results), 1)
                        _, cursor_id = results[0]
                        env.assertNotEqual(cursor_id, 0)
                        # A second read exercises reset of the per-chunk encoded state.
                        _, next_id = client.execute_command(prefix + 'CURSOR', 'READ', 'idx', cursor_id, 'COUNT', 1)
                        env.assertEqual(next_id, cursor_id)
                        client.execute_command(prefix + 'CURSOR', 'DEL', 'idx', cursor_id)
                    env.assertEqual(cursor_count(), 0)
                    if action != 'disconnect':
                        env.assertTrue(client.ping())
                finally:
                    control.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                    thread.join(timeout=5)
                    control.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                    client.close()
                    pool.disconnect()


def test_background_cursor_resp2():
    """Initial and subsequent cursor replies publish once or clean up after cancellation."""
    _exercise_cursor(2)


def test_background_cursor_resp3():
    """RESP3 cursor IDs are usable only when their reply is accepted."""
    _exercise_cursor(3)
