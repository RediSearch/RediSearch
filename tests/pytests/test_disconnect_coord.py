# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
from redis.exceptions import ConnectionError
from test_blocked_client_timeout import (
    _coord_cursor_total,
    _get_coord_req_ctx_free_count,
    is_client_blocked,
)


class TestCoordinatorDisconnect:
    def __init__(self):
        skipTest(cluster=False)
        self.env = Env(moduleArgs='WORKERS 1 TIMEOUT 0', protocol=3)
        skipIfNoEnableAssert(self.env)
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
        conn = getConnectionByEnv(self.env)
        for i in range(100):
            conn.execute_command('HSET', f'doc:{i}', 'name', f'name{i}')
        waitForIndex(self.env, 'idx')

    def _start_query(self, command):
        client = self.env.getConnection()
        connection = client.connection_pool.get_connection()
        connection.send_command('CLIENT', 'ID')
        client_id = connection.read_response()
        connection.send_command(*command)
        wait_for_condition(
            lambda: (is_client_blocked(client, str(client_id)), {}),
            'Query client did not block', timeout=10)
        return client, connection, client_id

    def _kill_query(self, query):
        client, connection, client_id = query
        try:
            self.env.expect('CLIENT', 'KILL', 'ID', client_id).equal(1)
            try:
                connection.read_response()
                self.env.assertTrue(False, message='Disconnected query returned a reply')
            except ConnectionError:
                pass
        finally:
            client.connection_pool.release(connection)

    def _wait_for_free(self, before):
        wait_for_condition(
            lambda: (_get_coord_req_ctx_free_count(self.env) > before, {}),
            'Disconnected coordinator request was not released', timeout=10)

    def _wait_at(self, connection, point):
        wait_for_condition(
            lambda: (connection.execute_command(
                debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
            f'Worker did not reach {point}', timeout=10)

    def test_aggregate_disconnect_wakes_shard_reader(self):
        """Free the coordinator request while every shard remains parked."""
        env = self.env
        point = 'BeforeCursorReadSendChunk'
        shards = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
        for policy in ('fail', 'return-strict'):
            run_command_on_all_shards(env, config_cmd(), 'SET', 'ON_TIMEOUT', policy)
            for shard in shards:
                shard.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', point)
            try:
                before = _get_coord_req_ctx_free_count(env)
                query = self._start_query(['FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 100])
                for shard in shards:
                    self._wait_at(shard, point)
                self._kill_query(query)
                self._wait_for_free(before)
            finally:
                for shard in shards:
                    shard.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                    shard.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')

    def test_aggregate_disconnect_before_request_creation(self):
        env = self.env
        for policy in ('fail', 'return-strict'):
            env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', policy).ok()
            before = _get_coord_req_ctx_free_count(env)
            env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
            try:
                query = self._start_query(['FT.AGGREGATE', 'idx', '*'])
                self._kill_query(query)
            finally:
                env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
            self._wait_for_free(before)
            env.expect('FT.AGGREGATE', 'idx', '*').noError()

    def _disconnect_cursor(self, read):
        env = self.env
        point = 'BeforeCursorReadSendChunk'
        for policy in ('fail', 'return-strict'):
            run_command_on_all_shards(env, config_cmd(), 'SET', 'ON_TIMEOUT', policy)
            baseline = _coord_cursor_total(env)
            if read:
                _, cid = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 1)
                env.assertNotEqual(cid, 0)
                command = ['FT.CURSOR', 'READ', 'idx', cid, 'COUNT', 1]
            else:
                command = ['FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 1]
            before = _get_coord_req_ctx_free_count(env)
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
            try:
                query = self._start_query(command)
                self._wait_at(env.getConnection(), point)
                self._kill_query(query)
                self._wait_for_free(before)
                env.assertEqual(_coord_cursor_total(env), baseline)
            finally:
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()

    def test_disconnect_initial_cursor(self):
        self._disconnect_cursor(read=False)

    def test_disconnect_cursor_read(self):
        self._disconnect_cursor(read=True)

    def test_search_disconnect_during_reduce(self):
        env = self.env
        for policy in ('fail', 'return-strict', 'return'):
            env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', policy).ok()
            setPauseBeforeReduce(env, 1)
            try:
                query = self._start_query(['FT.SEARCH', 'idx', '*'])
                wait_for_condition(
                    lambda: (getIsCoordReducePaused(env) == 1, {}),
                    'Search reducer did not pause', timeout=10)
                self._kill_query(query)
                wait_for_condition(
                    lambda: (getIsCoordReducePaused(env) == 0, {}),
                    'Disconnect did not cancel the search reducer', timeout=10)
            finally:
                resetCoordReduceDebug(env)
            env.expect('FT.SEARCH', 'idx', '*').noError()
