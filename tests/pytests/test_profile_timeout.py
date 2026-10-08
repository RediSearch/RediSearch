# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import threading

from common import *
from test_blocked_client_timeout import wait_for_blocked_query_client

TIMEOUT_WARNING = 'Timeout limit was reached'


def _setup(protocol):
    # Workers exercise the timer path; TIMEOUT 0 keeps setup independent of host speed.
    env = Env(protocol=protocol, moduleArgs='ON_TIMEOUT FAIL TIMEOUT 0 WORKERS 2',
              enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i in range(30):
        conn.execute_command('HSET', f'doc:{{{i}}}', 't', 'hello', 'n', i)
    env.expect('FT.SEARCH', 'idx', '*').noError()
    return env


def _start(client, command):
    result = {}

    def run():
        try:
            result['reply'] = client.execute_command(*command)
        except Exception as error:
            result['error'] = error

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, result


def _wait_point(client, point):
    wait_for_condition(
        lambda: (client.execute_command(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
        f'Worker did not reach {point}', timeout=10)


def _fire(env, client, command):
    client_id = wait_for_blocked_query_client(client, command, timeout=10)
    env.assertEqual(client.execute_command(debug_cmd(), 'QUERY_CONTROLLER',
                                           'FIRE_PROFILE_TIMEOUT', client_id), 1)
    # Firing removes the timer: the normal deadline must not signal a second time.
    env.assertEqual(client.execute_command(debug_cmd(), 'QUERY_CONTROLLER',
                                           'FIRE_PROFILE_TIMEOUT', client_id), 0)
    return client_id


def _finish(env, thread, result):
    thread.join(timeout=10)
    env.assertFalse(thread.is_alive(), message=result)
    env.assertTrue('error' not in result, message=result)
    return result['reply']


def _parts(env, reply):
    if env.protocol == 3:
        env.assertEqual(reply['Results']['results'], [], message=reply)
        return reply['Profile']
    env.assertEqual(len(reply), 2, message=reply)
    env.assertEqual(len(reply[0]), 1, message=reply)
    return to_dict(reply[1])


def _query(kind, timeout=60000):
    return ['FT.PROFILE', 'idx', kind, 'QUERY', '*', 'LIMIT', 0, 100, 'TIMEOUT', timeout]


def _shard_timeouts(protocol):
    env = _setup(protocol)
    clients = ([env.getConnection(i) for i in range(1, env.shardsCount + 1)]
               if env.isCluster() else [env.getConnection()])
    command = '_FT.PROFILE' if env.isCluster() else 'FT.PROFILE'
    point = 'BeforeSpecLock'
    for kind in ('SEARCH', 'AGGREGATE'):
        for client in clients:
            client.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', point)
        thread, result = _start(env.getConnection(), _query(kind))
        try:
            for client in clients:
                _wait_point(client, point)
                _fire(env, client, command)
            env.assertTrue(thread.is_alive(), message='Timer replied before workers finished profiling')
        finally:
            for client in clients:
                client.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                client.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        reply = _finish(env, thread, result)
        profile = _parts(env, reply)
        env.assertEqual(len(profile['Shards']), len(clients), message=reply)
        for shard in profile['Shards']:
            shard = shard if isinstance(shard, dict) else to_dict(shard)
            env.assertContains(TIMEOUT_WARNING, shard['Warning'], message=reply)
        env.expect('FT.PROFILE', 'idx', kind, 'QUERY', '*', 'TIMEOUT', 0).noError()


def test_profile_shard_timeouts_resp2():
    """All shards stop while queued, then return real profiles in the RESP2 envelope."""
    _shard_timeouts(2)


def test_profile_shard_timeouts_resp3():
    """All shards stop while queued, then return real profiles in the RESP3 envelope."""
    _shard_timeouts(3)


def _coordinator_timeout(protocol, queued):
    env = _setup(protocol)
    clients = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
    point = 'BeforeSpecLock'
    for kind in ('SEARCH', 'AGGREGATE'):
        if queued:
            env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
            wait_for_condition(lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'IS_PAUSED'), {}),
                               'Coordinator pool did not pause', timeout=10)
        else:
            for client in clients:
                client.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', point)
        thread, result = _start(env.getConnection(), _query(kind))
        try:
            if not queued:
                for client in clients:
                    _wait_point(client, point)
            _fire(env, env.getConnection(), 'FT.PROFILE')
            env.assertTrue(thread.is_alive(), message='Timer must leave profile collection active')
        finally:
            if queued:
                env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
            else:
                for client in clients:
                    client.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                    client.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        reply = _finish(env, thread, result)
        profile = _parts(env, reply)
        env.assertEqual(len(profile['Shards']), env.shardsCount, message=reply)
        coord = profile['Coordinator']
        coord = coord if isinstance(coord, dict) else to_dict(coord)
        env.assertContains(TIMEOUT_WARNING, coord['Warning'], message=reply)


@skip(cluster=False)
def test_profile_coordinator_queue_timeout_resp2():
    """An expired queued coordinator request still constructs and returns a profile."""
    _coordinator_timeout(2, queued=True)


@skip(cluster=False)
def test_profile_coordinator_queue_timeout_resp3():
    """An expired queued coordinator request still constructs and returns a profile."""
    _coordinator_timeout(3, queued=True)


@skip(cluster=False)
def test_profile_collection_after_timeout_resp2():
    """Coordinator timeout preserves pending shard profiles under RESP2."""
    _coordinator_timeout(2, queued=False)


@skip(cluster=False)
def test_profile_collection_after_timeout_resp3():
    """Coordinator timeout preserves pending shard profiles under RESP3."""
    _coordinator_timeout(3, queued=False)


def _cancel_collection(protocol):
    env = _setup(protocol)
    clients = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
    point = 'BeforeSpecLock'
    for client in clients:
        client.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', point)
    before = env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'GET_COORD_REQ_CTX_FREE_COUNT')
    thread, result = _start(env.getConnection(), _query('AGGREGATE'))
    try:
        for client in clients:
            _wait_point(client, point)
        client_id = _fire(env, env.getConnection(), 'FT.PROFILE')
        env.expect('CLIENT', 'KILL', 'ID', client_id).equal(1)
        thread.join(timeout=10)
        env.assertFalse(thread.is_alive(), message=result)
        env.assertTrue(isinstance(result.get('error'), redis_exceptions.ConnectionError), message=result)
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'GET_COORD_REQ_CTX_FREE_COUNT') > before, {}),
            'Cancellation did not release the coordinator while shard profiles were pending', timeout=10)
    finally:
        for client in clients:
            client.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
            client.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')


@skip(cluster=False)
def test_profile_cancel_collection_resp2():
    """Disconnect interrupts profile collection even after the execution timer fired."""
    _cancel_collection(2)


@skip(cluster=False)
def test_profile_cancel_collection_resp3():
    """Disconnect interrupts profile collection even after the execution timer fired."""
    _cancel_collection(3)


def _buffer_timeout(protocol):
    env = _setup(protocol)
    for kind in ('SEARCH', 'AGGREGATE'):
        env.expect(debug_cmd(), 'QUERY_CONTROLLER', 'SET_PAUSE_AFTER_AGGREGATE_RESULT', 1).ok()
        thread, result = _start(env.getConnection(), _query(kind))
        try:
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'GET_IS_AGGREGATE_RESULTS_PAUSED'), {}),
                'Worker did not buffer a result', timeout=10)
            _fire(env, env.getConnection(), 'FT.PROFILE')
            reply = _finish(env, thread, result)
            profile = _parts(env, reply)
            shard = profile['Shards'][0]
            shard = shard if isinstance(shard, dict) else to_dict(shard)
            env.assertContains(TIMEOUT_WARNING, shard['Warning'], message=reply)
        finally:
            env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'SET_PAUSE_AFTER_AGGREGATE_RESULT', 0)
            if env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'GET_IS_AGGREGATE_RESULTS_PAUSED'):
                env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'SET_AGGREGATE_RESULTS_RESUME')
            thread.join(timeout=10)


@skip(cluster=True)
def test_profile_discards_buffered_rows_resp2():
    """The soft deadline discards an already-buffered FAIL row without losing diagnostics."""
    _buffer_timeout(2)


@skip(cluster=True)
def test_profile_discards_buffered_rows_resp3():
    """The soft deadline discards an already-buffered FAIL row without losing diagnostics."""
    _buffer_timeout(3)


def _cursor_timeout(protocol):
    env = _setup(protocol)
    client = env.getConnection(1)
    first = client.execute_command('_FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',
                                   'WITHCURSOR', 'COUNT', 1, 'TIMEOUT', 60000)
    cursor = first[1]
    env.assertNotEqual(cursor, 0, message=first)
    point = 'BeforeCursorReadSendChunk'
    client.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', point)
    thread, result = _start(client, ['_FT.CURSOR', 'READ', 'idx', cursor])
    try:
        _wait_point(client, point)
        _fire(env, client, '_FT.CURSOR|READ')
        reply = _finish(env, thread, result)
        env.assertEqual(reply[1], 0, message=reply)
        if protocol == 3:
            env.assertEqual(reply[0]['Results']['results'], [], message=reply)
            profile = reply[0]['Profile']
        else:
            env.assertEqual(len(reply[0]), 1, message=reply)
            profile = to_dict(reply[2])
        shard = profile['Shards'][0]
        shard = shard if isinstance(shard, dict) else to_dict(shard)
        env.assertContains(TIMEOUT_WARNING, shard['Warning'], message=reply)
        env.assertEqual(shard['Internal cursor reads'], 2, message=reply)
    finally:
        client.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
        client.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        thread.join(timeout=10)
    env.expect('_FT.CURSOR', 'READ', 'idx', cursor).error().contains('Cursor not found')


@skip(cluster=False)
def test_profile_internal_cursor_timeout_resp2():
    """A shard cursor read returns its final profile and closes after a soft deadline."""
    _cursor_timeout(2)


@skip(cluster=False)
def test_profile_internal_cursor_timeout_resp3():
    """A shard cursor read returns its final profile and closes after a soft deadline."""
    _cursor_timeout(3)


@skip(cluster=True)
def test_profile_real_timer():
    """Exercise Redis timer delivery while a worker is held before execution."""
    env = _setup(3)
    point = 'BeforeSpecLock'
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
    before = env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'GET_PROFILE_TIMEOUT_COUNT')
    thread, result = _start(env.getConnection(), _query('AGGREGATE', timeout=1))
    try:
        _wait_point(env.getConnection(), point)
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'GET_PROFILE_TIMEOUT_COUNT') > before, {}),
            'Redis did not deliver the profile timer', timeout=10)
        env.assertTrue(thread.is_alive(), message='Timer unexpectedly completed the reply')
    finally:
        env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
        env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
    reply = _finish(env, thread, result)
    profile = _parts(env, reply)
    env.assertContains(TIMEOUT_WARNING, profile['Shards'][0]['Warning'], message=reply)
