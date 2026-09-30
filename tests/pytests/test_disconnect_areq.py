# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
from test_hybrid_internal import get_shard_slot_ranges


class TestAREQDisconnect:
    def __init__(self):
        self.env = Env(protocol=3, moduleArgs='WORKERS 1 TIMEOUT 0')
        skipIfNoEnableAssert(self.env)
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
        conn = getConnectionByEnv(self.env)
        for i in range(100):
            conn.execute_command('HSET', f'doc:{i}', 'name', f'hello{i}')
        self.command_prefix = '_FT.' if self.env.isCluster() else 'FT.'
        if self.env.isCluster():
            self.env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')
            _, self.slots = get_shard_slot_ranges(self.env)[0]

    def _cursor_total(self):
        info = self.env.cmd(self.command_prefix + 'INFO', 'idx')
        return int(info['cursor_stats']['global_total'])

    def _disconnect(self, query, sync_point, kill, queued=False):
        """Require worker completion without releasing its cancellation-aware pause."""
        env = self.env
        client = env.getConnection()
        connection = client.connection_pool.get_connection()
        wait_for_condition(
            lambda: (getWorkersThpoolStats(env)['numJobsInProgress'] == 0 and
                     getWorkersThpoolStats(env)['totalPendingJobs'] == 0, {}),
            'Previous worker job did not finish', timeout=5)
        before = getWorkersThpoolStats(env)['totalJobsDone']
        workers_paused = queued
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()
        if queued:
            env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
        try:
            connection.send_command('CLIENT', 'ID')
            client_id = connection.read_response()
            if env.isCluster():
                connection.send_command('DEBUG', 'MARK-INTERNAL-CLIENT')
                env.assertEqual(connection.read_response(), 'OK')
            connection.send_command(*query)
            if queued:
                wait_for_condition(
                    lambda: (any(int(c['id']) == client_id and 'b' in c['flags']
                                 for c in client.client_list()), {}),
                    'Query did not enter the worker queue', timeout=5)
            else:
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    f'Query did not reach {sync_point}', timeout=5)

            if kill:
                env.expect('CLIENT', 'KILL', 'ID', client_id).equal(1)
            else:
                connection.disconnect()
                wait_for_condition(
                    lambda: (all(int(c['id']) != client_id for c in client.client_list()), {}),
                    'Server did not observe the closed connection', timeout=5)
            if workers_paused:
                env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
                workers_paused = False
            wait_for_condition(
                lambda: (getWorkersThpoolStats(env)['totalJobsDone'] > before, {}),
                'Disconnect did not stop the worker', timeout=5)
            wait_for_condition(
                lambda: (self._cursor_total() == 0, {'cursors': self._cursor_total()}),
                'Disconnected request retained a cursor', timeout=5)
        finally:
            # Always release debug hooks, including when testing an unfixed module.
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            if workers_paused:
                env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
            connection.disconnect()
            client.connection_pool.release(connection)

    def _test_disconnect(self, kind, queued=False):
        env = self.env
        for policy in ('fail', 'return-strict'):
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy).ok()
            for kill in (False, True):
                if kind == 'search':
                    query = [self.command_prefix + 'SEARCH', 'idx', '*']
                else:
                    query = [self.command_prefix + 'AGGREGATE', 'idx', '*']
                sync_point = 'BeforeAggregateResultsClaim'
                if kind in ('initial_cursor', 'cursor_read'):
                    query += ['WITHCURSOR', 'COUNT', '1']
                    sync_point = 'BeforeCursorReadSendChunk'
                if env.isCluster():
                    query += ['_SLOTS_INFO', self.slots]
                if kind == 'cursor_read':
                    _, cursor_id = env.cmd(*query)
                    env.assertNotEqual(cursor_id, 0)
                    query = [self.command_prefix + 'CURSOR', 'READ', 'idx', cursor_id, 'COUNT', '1']
                self._disconnect(query, sync_point, kill, queued)

    def test_search_disconnect(self):
        self._test_disconnect('search')

    def test_aggregate_disconnect(self):
        self._test_disconnect('aggregate')

    def test_initial_cursor_disconnect(self):
        self._test_disconnect('initial_cursor')

    def test_cursor_read_disconnect(self):
        self._test_disconnect('cursor_read')

    def test_queued_query_disconnect(self):
        self._test_disconnect('aggregate', queued=True)

    def test_queued_cursor_read_disconnect(self):
        self._test_disconnect('cursor_read', queued=True)


@skip(cluster=True)
def test_areq_disconnect_callbacks_normal_resp2():
    """Installing disconnect callbacks preserves normal RESP2/profile replies."""
    env = Env(protocol=2, moduleArgs='WORKERS 1 TIMEOUT 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    env.cmd('HSET', 'doc', 'name', 'hello')
    for policy in ('fail', 'return-strict'):
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy).ok()
        env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([1, 'doc'])
        for command in ('SEARCH', 'AGGREGATE'):
            reply = env.cmd('FT.PROFILE', 'idx', command, 'QUERY', '*')
            env.assertEqual(reply[0][0], 1)
            env.assertTrue(len(reply[1]) > 0)
