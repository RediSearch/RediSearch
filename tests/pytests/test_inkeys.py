# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import os
import threading
from unittest.mock import patch

from common import *
from RLTest import Env


def _queued_reindex_with_key_map(pause_backfill):
    """A reindex before the execution lock must not invalidate INKEYS membership."""
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0')
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC',
               'SORTABLE', 'NOINDEX').ok()
    env.cmd('HSET', 'doc', 't', 'hello', 'n', 1)
    old_id = env.cmd(debug_cmd(), 'DOCIDTOID', 'idx', 'doc')
    if pause_backfill:
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'KeyMapBeforeBatch').ok()
    else:
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, 'doc', 'NOCONTENT').equal([1, 'doc'])
        env.expect(debug_cmd(), 'BG_SCAN_CONTROLLER', 'TERMINATE_BG_POOL').ok()
    point = 'BeforeSpecLock'
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
    results = []
    conn = env.getConnection()

    def query():
        try:
            results.append(conn.execute_command('FT.SEARCH', 'idx', 'hello',
                                                 'INKEYS', 1, 'doc', 'NOCONTENT'))
        except Exception as exc:
            results.append(exc)

    thread = threading.Thread(target=query, daemon=True)
    thread.start()
    try:
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
            'query did not reach the execution handoff')
        env.cmd('HSET', 'doc', 'n', 2)
        env.assertNotEqual(env.cmd(debug_cmd(), 'DOCIDTOID', 'idx', 'doc'), old_id)
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
        thread.join(timeout=10)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.assertFalse(thread.is_alive(), message='query did not complete')
    env.assertEqual(results, [[1, 'doc']])
    env.stop()


def test_doc_key_map_lifecycle():
    """The map follows initial scans, updates, rename, deletion and reload."""
    # Each shard must maintain its own table, including during an initial scan.
    env = Env(moduleArgs='WORKERS 1')
    conn = getConnectionByEnv(env)
    key = '{doc}:a'
    renamed = '{doc}:b'
    binary = '{doc}:a\0suffix'
    conn.execute_command('HSET', key, 't', 'hello', 'n', 1)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, '{doc}:', 'SCHEMA', 't', 'TEXT',
               'n', 'NUMERIC', 'SORTABLE').ok()
    waitForIndex(env, 'idx')

    for _ in env.reloadingIterator():
        env.assertEqual(float(index_info(env, 'idx')['key_table_size_mb']), 0)
        env.expect('FT.SEARCH', 'idx', 'hello', 'INKEYS', 3, key, key, 'missing',
                   'NOCONTENT').equal([1, key])
        conn.execute_command('HSET', key, 't', 'hello', 'n', 2)
        env.expect('FT.SEARCH', 'idx', '@n:[2 2]', 'INKEYS', 1, key,
                   'NOCONTENT').equal([1, key])
        conn.execute_command('RENAME', key, renamed)
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([0])
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, renamed,
                   'NOCONTENT').equal([1, renamed])
        conn.execute_command('HSET', binary, 't', 'hello', 'n', 3)
        env.expect('FT.SEARCH', 'idx', '@n:[3 3]', 'INKEYS', 1, binary,
                   'NOCONTENT').equal([1, binary])
        conn.execute_command('DEL', binary, renamed)
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 2, binary, renamed,
                   'NOCONTENT').equal([0])
        conn.execute_command('HSET', key, 't', 'hello', 'n', 1)

    env.expect('FT.CREATE', 'idx2', 'PREFIX', 1, '{doc}:', 'FILTER', '@n > 0',
               'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC').ok()
    waitForIndex(env, 'idx2')
    conn.execute_command('HSET', key, 'n', -1)
    env.expect('FT.SEARCH', 'idx2', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([1, key])
    conn.execute_command('HSET', key, 'n', 1)
    env.expect('FT.DROPINDEX', 'idx').ok()
    env.expect('FT.SEARCH', 'idx2', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([1, key])
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, '{doc}:', 'SCHEMA', 't', 'TEXT').ok()
    waitForIndex(env, 'idx')
    conn.execute_command('PEXPIREAT', key, 1)
    env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, key, 'NOCONTENT').equal([0])
    env.stop()


@skip(cluster=True)
def test_doc_key_map_memory_and_inline():
    """First INKEYS activates the map and supports inline execution."""
    # WORKERS 0 also exercises resolution without a thread-safe context lock.
    env = Env(moduleArgs='WORKERS 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.cmd('HSET', 'doc', 't', 'hello')
    env.expect('FT.SEARCH', 'idx', 'hello', 'INKEYS', 1, 'doc',
               'NOCONTENT').equal([1, 'doc'])
    env.assertGreater(float(index_info(env, 'idx')['key_table_size_mb']), 0)
    env.assertContains('IDS {1,}', env.cmd('FT.EXPLAIN', 'idx', 'hello',
                                         'INKEYS', 1, 'doc'))
    env.assertEqual(env.cmd('EVAL',
                           "return redis.call('FT.SEARCH', 'idx', 'hello', 'INKEYS', 1, 'doc', 'NOCONTENT')",
                           0), [1, 'doc'])
    env.cmd('HSET', '', 't', 'hello')
    env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, '', 'NOCONTENT').equal([1, ''])
    env.stop()


@skip(cluster=True)
def test_inkeys_timeout_before_resolution():
    """Timeout callbacks can finish while INKEYS is waiting for its execution lock."""
    import redis

    for policy in ('fail', 'return-strict'):
        env = Env(moduleArgs='WORKERS 1 TIMEOUT 0')
        skipIfNoEnableAssert(env)
        env.cmd('FLUSHALL')
        env.expect('CONFIG', 'SET', 'search-on-timeout', policy).ok()
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
        env.cmd('HSET', 'doc', 't', 'hello')
        conn = redis.Redis(single_connection_client=True,
                           decode_responses=True,
                           **{k: v for k, v in env.getConnection().connection_pool.connection_kwargs.items()
                              if k != 'decode_responses'})
        client_id = conn.client_id()
        point = 'BeforeSpecLock'
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
        results = []

        def query():
            try:
                results.append(conn.execute_command('FT.SEARCH', 'idx', '*',
                                                     'INKEYS', 1, 'doc', 'NOCONTENT'))
            except Exception as exc:
                results.append(str(exc))

        thread = threading.Thread(target=query, daemon=True)
        thread.start()
        try:
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
                'query did not reach the execution handoff')
            env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
            thread.join(timeout=10)
            env.assertFalse(thread.is_alive(), message='timeout callback did not reply')
        finally:
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
            thread.join(timeout=10)
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        if policy == 'fail':
            env.assertEqual(len(results), 1)
            env.assertContains('Timeout', results[0])
        else:
            env.assertEqual(results, [[0]])
        env.expect('PING').equal(True)
        conn.close()
        env.stop()


@skip(cluster=True)
def test_lazy_key_map_activation():
    for workers in (0, 2):
        env = Env(moduleArgs=f'WORKERS {workers} TIMEOUT 0')
        skipIfNoEnableAssert(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
        for i in range(2050):
            env.cmd('HSET', f'doc:{i}', 't', 'hello')
        env.expect('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 0).equal([2050])
        env.assertEqual(float(index_info(env, 'idx')['key_table_size_mb']), 0)
        for point in ('KeyMapBeforeBatch', 'KeyMapBuildComplete'):
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
        aggregate = ('FT.AGGREGATE', 'idx', '*', 'INKEYS', 4,
                     'renamed', 'new', 'missing', 'renamed',
                     'LOAD', 1, '@__key', 'SORTBY', 2, '@__key', 'ASC')
        aggregate_result = [2, ['__key', 'new'], ['__key', 'renamed']]
        try:
            env.expect('FT.AGGREGATE', 'idx', '*', 'INKEYS', 1, 'doc:0',
                       'LOAD', 1, '@__key').equal([1, ['__key', 'doc:0']])
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', 'KeyMapBeforeBatch') == 1, {}),
                'aggregate did not start backfill')
            # The partial map cannot answer queries while the builder is parked.
            env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, 'doc:0',
                       'NOCONTENT').equal([1, 'doc:0'])
            env.cmd('RENAME', 'doc:0', 'renamed')
            env.cmd('DEL', 'doc:1')
            env.cmd('HSET', 'new', 't', 'hello')
            env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 2, 'renamed', 'new',
                       'NOCONTENT').equal([2, 'renamed', 'new'])
            env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 2, 'doc:0', 'doc:1',
                       'NOCONTENT').equal([0])
            env.expect(*aggregate).equal(aggregate_result)
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', 'KeyMapBeforeBatch')
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', 'KeyMapBuildComplete') == 1, {}),
                'backfill did not finish')
            env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, 'renamed',
                       'NOCONTENT').equal([1, 'renamed'])
            env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, 'new',
                       'NOCONTENT').equal([1, 'new'])
            env.expect(*aggregate).equal(aggregate_result)
            env.assertGreater(float(index_info(env, 'idx')['key_table_size_mb']), 0)
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', 'KeyMapBuildComplete')
        finally:
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.stop()


@skip(cluster=True)
def test_lazy_key_map_drop_during_build():
    env = Env(moduleArgs='WORKERS 0')
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.cmd('HSET', 'doc', 't', 'hello')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'KeyMapBeforeBatch').ok()
    try:
        env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, 'doc', 'NOCONTENT').equal([1, 'doc'])
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', 'KeyMapBeforeBatch') == 1, {}),
            'backfill did not start')
        env.expect('FT.DROPINDEX', 'idx').ok()
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.assertTrue(env.cmd('PING'))
    env.stop()


@skip(cluster=True)
def test_lazy_key_map_shutdown_during_build():
    with patch.dict(os.environ, {'RS_GLOBAL_DTORS': '1'}):
        env = Env(moduleArgs='WORKERS 0 _FREE_RESOURCE_ON_THREAD TRUE')
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.cmd('HSET', 'doc', 't', 'hello')
    # Reindex-pool teardown releases this hook after index ownership is dropped.
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'KeyMapBuildComplete').ok()
    env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 1, 'doc', 'NOCONTENT').equal([1, 'doc'])
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', 'KeyMapBuildComplete') == 1, {}),
        'backfill did not reach completion')
    env.stop()
    env.assertTrue(env.checkExitCode())


@skip(cluster=True)
def test_inkeys_resolves_after_queued_reindex():
    _queued_reindex_with_key_map(True)


@skip(cluster=True)
def test_inkeys_resolves_after_queued_reindex_ready():
    _queued_reindex_with_key_map(False)


@skip(cluster=True)
def test_lazy_key_map_expired_fallback():
    """Metadata reads under the spec lock must not unlink lazily expired keys."""
    for workers in (0, 2):
        env = Env(moduleArgs=f'WORKERS {workers} TIMEOUT 0')
        skipIfNoEnableAssert(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
        env.cmd('HSET', 'expired', 't', 'hello')
        env.cmd('HSET', 'live', 't', 'hello')
        env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', 0)
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'KeyMapBeforeBatch').ok()
        try:
            now = env.cmd('TIME')
            deadline = int(now[0]) * 1000 + int(now[1]) // 1000 + 100
            env.cmd('PEXPIREAT', 'expired', deadline)
            # TIME observes the TTL deadline without touching the key or expiring it.
            wait_for_condition(
                lambda: (int(env.cmd('TIME')[0]) * 1000 > deadline, {}),
                'key TTL did not elapse')
            env.assertEqual(env.cmd('DBSIZE'), 2)
            env.expect('FT.SEARCH', 'idx', '*', 'INKEYS', 2, 'expired', 'live',
                       'NOCONTENT').equal([1, 'live'])
            env.assertContains('IDS {2,}', env.cmd('FT.EXPLAIN', 'idx', '*',
                                                 'INKEYS', 2, 'expired', 'live'))
            env.assertEqual(env.cmd('DBSIZE'), 2)
        finally:
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', 1)
        env.stop()


@skip(cluster=True)
def test_lazy_key_map_profile_gil():
    """Fallback planning contributes to the reported worker GIL interval."""
    from test_profile import extract_profile_coordinator_and_shards

    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0', protocol=3)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.cmd('HSET', 'doc', 't', 'hello')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'KeyMapBeforeBatch').ok()
    # Hold the measured interval open so pre-dispatch time cannot hide missing accounting.
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'KeyMapFallbackGIL', 50).ok()
    try:
        result = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*',
                         'INKEYS', 1, 'doc', 'NOCONTENT')
        _, shards = extract_profile_coordinator_and_shards(env, result)
        shard = shards[0]
        env.assertGreaterEqual(float(shard['Total GIL time']), 40, message=shard)
        env.assertGreaterEqual(float(shard['Total GIL time']),
                               float(shard['Pipeline creation time']), message=shard)
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.stop()
