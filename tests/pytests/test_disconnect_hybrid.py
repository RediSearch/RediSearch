# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import threading

from redis.exceptions import ConnectionError

from common import *
from test_blocked_client_timeout import (
    _get_coord_req_ctx_free_count,
    _internal_hybrid_cursor_map,
    _setup_hybrid_index,
    wait_for_client_blocked,
)
from test_hybrid_internal import get_shard_slot_ranges


def _query(blob, internal=False, profile=False):
    prefix = '_' if internal else ''
    command = ([prefix + 'FT.PROFILE', 'hybrid_idx', 'HYBRID', 'QUERY'] if profile else
               [prefix + 'FT.HYBRID', 'hybrid_idx'])
    return command + ['SEARCH', '*', 'VSIM', '@embedding', '$BLOB',
                      'PARAMS', '2', 'BLOB', blob, 'TIMEOUT', '0']


def _start_query(client, command, internal=False):
    """Use one raw connection so redis-py cannot retry the killed query."""
    connection = client.connection_pool.get_connection()
    connection.send_command('CLIENT', 'ID')
    client_id = connection.read_response()
    if internal:
        connection.send_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        connection.read_response()
    outcome = []

    def run():
        try:
            connection.send_command(*command)
            outcome.append(('reply', connection.read_response()))
        except ConnectionError:
            outcome.append(('disconnected',))
        except Exception as exc:
            outcome.append(('error', str(exc)))
        finally:
            connection.disconnect()
            client.connection_pool.release(connection)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return client_id, thread, outcome


def _kill(env, client, client_id, thread, outcome):
    env.assertEqual(client.execute_command('CLIENT', 'KILL', 'ID', client_id), 1)
    thread.join(timeout=10)
    env.assertFalse(thread.is_alive(), message='Disconnected HYBRID client did not finish')
    env.assertEqual(outcome, [('disconnected',)])


def _wait_idle(client):
    wait_for_condition(
        lambda: (to_dict(client.execute_command(debug_cmd(), 'WORKERS', 'STATS'))[
            'numJobsInProgress'] == 0, {}),
        'Disconnected HYBRID worker did not finish', timeout=10)


def _cursor_count(client):
    # The killed query connection may be reused after reconnecting.
    with client.client() as connection:
        connection.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        info = to_dict(connection.execute_command('_FT.INFO', 'hybrid_idx'))
    return int(to_dict(info['cursor_stats'])['index_total'])


@skip(cluster=True)
def test_standalone_hybrid_disconnect():
    """Both policies stop a running HYBRID or PROFILE without a clock deadline."""
    env = Env(moduleArgs='WORKERS 2', protocol=3, enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    blob = _setup_hybrid_index(env)
    waitForIndex(env, 'hybrid_idx')
    client = env.getConnection()
    sync_point = 'BeforeHybridResultsClaim'
    for policy in ('fail', 'return-strict'):
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy)
        for profile in (False, True):
            env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)
            client_id, thread, outcome = _start_query(client, _query(blob, profile=profile))
            try:
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    'HYBRID did not reach its tail pipeline')
                _kill(env, client, client_id, thread, outcome)
                # Do not signal the sync point: only the published timeout flag releases it.
                _wait_idle(client)
            finally:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                thread.join(timeout=10)


@skip(cluster=False)
def test_internal_hybrid_disconnect_cursor_publication():
    """Disconnect before/after cursor publication releases the worker and cursors."""
    env = Env(moduleArgs='WORKERS 2', protocol=3, enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    blob = _setup_hybrid_index(env)
    waitForIndex(env, 'hybrid_idx')
    shard = env.getConnection(1)
    shard.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    slots = dict(get_shard_slot_ranges(env))[1]
    for policy in ('fail', 'return-strict'):
        shard.execute_command('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy)
        for phase in ('BEFORE', 'AFTER'):
            pause_cmd = f'SET_PAUSE_{phase}_HYBRID_STORE_CURSORS'
            shard.execute_command(debug_cmd(), 'QUERY_CONTROLLER', pause_cmd, 'true')
            baseline = _cursor_count(shard)
            command = _query(blob, internal=True) + [
                'WITHCURSOR', 'COUNT', '1', '_SLOTS_INFO', slots,
                '_COORD_DISPATCH_TIME', '1000000']
            client_id, thread, outcome = _start_query(shard, command, internal=True)
            try:
                wait_for_condition(
                    lambda: (shard.execute_command(debug_cmd(), 'QUERY_CONTROLLER',
                                                   'GET_IS_HYBRID_STORE_CURSORS_PAUSED') == 1, {}),
                    f'Internal HYBRID did not pause {phase.lower()} cursor publication')
                _kill(env, shard, client_id, thread, outcome)
                _wait_idle(shard)
                wait_for_condition(lambda: (_cursor_count(shard) == baseline, {}),
                                   'Disconnected HYBRID leaked its internal cursors', timeout=10)
            finally:
                shard.execute_command(debug_cmd(), 'QUERY_CONTROLLER', pause_cmd, 'false')
                thread.join(timeout=10)


@skip(cluster=False)
def test_coordinator_hybrid_disconnect_wakes_readers():
    """A killed coordinator finishes while shard cursor readers remain parked."""
    env = Env(moduleArgs='WORKERS 2', protocol=3, enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    blob = _setup_hybrid_index(env)
    waitForIndex(env, 'hybrid_idx')
    # Warm connections before measuring coordinator request destruction.
    env.cmd(*_query(blob))
    shards = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
    client = env.getConnection()
    sync_point = 'BeforeCursorReadSendChunk'
    for policy in ('fail', 'return-strict'):
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy)
        for profile in (False, True):
            for shard in shards:
                shard.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)
            freed = _get_coord_req_ctx_free_count(env)
            client_id, thread, outcome = _start_query(client, _query(blob, profile=profile))
            try:
                for shard in shards:
                    wait_for_condition(
                        lambda shard=shard: (shard.execute_command(
                            debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                        'Shard HYBRID cursor read did not pause')
                _kill(env, client, client_id, thread, outcome)
                wait_for_condition(
                    lambda: (_get_coord_req_ctx_free_count(env) > freed, {}),
                    'Disconnected HYBRID coordinator did not release its request', timeout=10)
            finally:
                for shard in shards:
                    shard.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
                    shard.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                thread.join(timeout=10)
                for shard in shards:
                    _wait_idle(shard)


@skip(cluster=False)
def test_internal_hybrid_cursor_read_disconnect():
    """Both subquery cursors use their cached policy for disconnect cancellation."""
    env = Env(moduleArgs='WORKERS 2', protocol=3, enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    blob = _setup_hybrid_index(env)
    waitForIndex(env, 'hybrid_idx')
    shard = env.getConnection(1)
    shard.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    slots = dict(get_shard_slot_ranges(env))[1]
    sync_point = 'BeforeCursorReadSendChunk'
    for policy in ('fail', 'return-strict'):
        shard.execute_command('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy)
        shard.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        cursors = _internal_hybrid_cursor_map(shard.execute_command(
            *_query(blob, internal=True), 'WITHCURSOR', 'COUNT', '1',
            '_SLOTS_INFO', slots, '_COORD_DISPATCH_TIME', '1000000'))
        shard.execute_command('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
        for component in ('SEARCH', 'VSIM'):
            cursor_id = cursors[component]
            env.assertNotEqual(cursor_id, 0)
            shard.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)
            command = ['_FT.CURSOR', 'READ', 'hybrid_idx', cursor_id]
            client_id, thread, outcome = _start_query(shard, command, internal=True)
            try:
                wait_for_condition(
                    lambda: (shard.execute_command(debug_cmd(), 'SYNC_POINT',
                                                  'IS_WAITING', sync_point) == 1, {}),
                    f'{component} cursor read did not pause')
                _kill(env, shard, client_id, thread, outcome)
                _wait_idle(shard)
            finally:
                shard.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
                shard.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                thread.join(timeout=10)


@skip(cluster=False)
def test_coordinator_hybrid_disconnect_before_pickup():
    """A queued HYBRID disconnect is retained until its dispatcher can clean up."""
    env = Env(moduleArgs='WORKERS 2', protocol=3, enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    blob = _setup_hybrid_index(env)
    waitForIndex(env, 'hybrid_idx')
    client = env.getConnection()
    for policy in ('fail', 'return-strict'):
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy)
        env.cmd(debug_cmd(), 'COORD_THREADS', 'PAUSE')
        freed = _get_coord_req_ctx_free_count(env)
        client_id, thread, outcome = _start_query(client, _query(blob))
        try:
            wait_for_client_blocked(client, client_id)
            _kill(env, client, client_id, thread, outcome)
            env.assertEqual(_get_coord_req_ctx_free_count(env), freed)
        finally:
            env.cmd(debug_cmd(), 'COORD_THREADS', 'RESUME')
            thread.join(timeout=10)
        wait_for_condition(
            lambda: (_get_coord_req_ctx_free_count(env) == freed + 1, {}),
            'Queued disconnected HYBRID did not release its request', timeout=10)
